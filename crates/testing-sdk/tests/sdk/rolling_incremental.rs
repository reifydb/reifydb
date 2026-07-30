// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::encoded::EncodedKey,
};
use reifydb_core::{interface::catalog::flow::FlowNodeId, row::Row as CoreRow};
use reifydb_flow::window::{
	accumulator::invertible::{LastValue, Moments},
	span::WindowCoord,
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		FFIOperatorAdapter,
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::RowView,
		windowed::{
			rolling::{RollingOperator, RollingRegistration},
			rolling_incremental::*,
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestRowBuilder},
	harness::FFIOperatorHarnessBuilder,
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType};

// Velocity-style operator: per-window value held last-write-wins; the
// cross-window Running accumulator is Moments over ALL window values, and
// the baseline is (Running minus the newest window) so the score
// (newest / baseline_mean) is computed in O(1) from the running moments.

#[derive(Clone, Debug, PartialEq)]
struct TestOut {
	group: String,
	recent: f64,
	baseline: f64,
	windows: u32,
}

row!(TestOut {
	group: String,
	recent: f64,
	baseline: f64,
	windows: u32
});

struct TestVelocity {
	capacity: usize,
}

impl RollingOperator for TestVelocity {
	type GroupKey = String;
	type WindowSlot = u64;
	type Accumulator = LastValue<f64>;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
		let group = row.utf8("group")?.to_string();
		let window_start = row.u64("window_start")?;
		let value = row.f64("value")?;
		Some((group, window_start, value))
	}

	// Reference combine over the buffer: baseline = mean of all-but-newest.
	fn combine(&self, group: &String, buffer: &BTreeMap<u64, LastValue<f64>>) -> Option<TestOut> {
		let (_, newest) = buffer.iter().next_back()?;
		let newest = (*newest.get()?) as f64;
		let mut sum = 0.0_f64;
		let mut count = 0u32;
		let total = buffer.len();
		for (i, accumulator) in buffer.values().enumerate() {
			if i + 1 == total {
				continue;
			}
			if let Some(v) = accumulator.get() {
				sum += *v;
				count += 1;
			}
		}
		let baseline = if count > 0 {
			sum / count as f64
		} else {
			0.0
		};
		Some(TestOut {
			group: group.clone(),
			recent: newest,
			baseline,
			windows: buffer.len() as u32,
		})
	}
}

impl RollingIncrementalOperator for TestVelocity {
	type Running = Moments;

	fn window_contribution(&self, window_value: &f64) -> f64 {
		*window_value
	}

	fn combine_running(
		&self,
		group: &String,
		running: &Moments,
		newest_value: &f64,
		_newest_coord: u64,
	) -> Option<TestOut> {
		// baseline = (running over ALL windows) minus the newest window.
		let total_count = running.count();
		let baseline_count = total_count.saturating_sub(1);
		let baseline = if baseline_count > 0 {
			(running.sum() - *newest_value) / baseline_count as f64
		} else {
			0.0
		};
		Some(TestOut {
			group: group.clone(),
			recent: *newest_value,
			baseline,
			windows: total_count as u32,
		})
	}
}

impl RollingRegistration for TestVelocity {
	const NAME: &'static str = "test_velocity_incremental";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self {
			capacity: 3,
		})
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}
}

fn input_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("value", ValueType::Float8),
	])
}

fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
	TestRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
		.with_shape(input_shape())
		.build()
}

#[test]
fn baseline_excludes_newest_window() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	// Windows 0=10, 60=20, 120=60. Newest=120 (recent 60); baseline =
	// mean(10, 20) = 15. Running moments maintained incrementally.
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 60, 20.0))
			.insert(input_row(3, "BTC", 120, 60.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("recent"), Some(60.0));
	assert_eq!(r.f64("baseline"), Some(15.0));
	assert_eq!(r.u32("windows"), Some(3));
}

#[test]
fn remove_clears_buffer_emits_remove() {
	// Removing the only window empties the rolling buffer; the driver must
	// withdraw the previously emitted output row (terminal Remove carrying the
	// prior value) rather than leak a ghost row - required for reorg-retraction
	// correctness of incremental rolling views.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(r.f64("recent"), Some(10.0));
}

#[test]
fn update_window_value_keeps_running_consistent() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 60, 20.0))
			.build())
		.expect("apply");
	// Update the NEWEST window (60) from 20 to 40. (Updating a buried
	// window like 0 would be dropped late, per the rolling contract.)
	// Running must reflect old->new: windows {10, 40}, newest=40,
	// baseline = mean(10) = 10.
	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(2, "BTC", 60, 20.0), input_row(2, "BTC", 60, 40.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("recent"), Some(40.0));
	assert_eq!(r.f64("baseline"), Some(10.0), "running updated old->new: baseline=mean(10)");
}

#[test]
fn eviction_drops_oldest_from_running() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	// Capacity 3; insert 4 windows. Window 0 (value 1) is evicted, so the
	// running moments must drop it: baseline = mean(2, 3) = 2.5, recent=4.
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 1.0))
			.insert(input_row(2, "BTC", 60, 2.0))
			.insert(input_row(3, "BTC", 120, 3.0))
			.insert(input_row(4, "BTC", 180, 4.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("recent"), Some(4.0));
	assert_eq!(r.f64("baseline"), Some(2.5), "evicted window 0 removed from running");
	assert_eq!(r.u32("windows"), Some(3));
}

fn millis(value: u64) -> Duration {
	Duration::from_milliseconds_const(value as i64)
}

// TestVelocity with sealing enabled, over a DateTime coordinate.
struct SealedVelocity;

impl RollingOperator for SealedVelocity {
	type GroupKey = String;
	type WindowSlot = DateTime;
	type Accumulator = LastValue<f64>;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		3
	}

	fn extract(&self, ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, DateTime, f64)> {
		let (group, window_start, value) = TestVelocity {
			capacity: 3,
		}
		.extract(ctx, row)?;
		Some((group, DateTime::from_millis(window_start), value))
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<DateTime, LastValue<f64>>) -> Option<TestOut> {
		let reindexed: BTreeMap<u64, LastValue<f64>> =
			buffer.iter().map(|(coord, a)| (coord.to_order(), a.clone())).collect();
		TestVelocity {
			capacity: 3,
		}
		.combine(group, &reindexed)
	}
}

impl RollingIncrementalOperator for SealedVelocity {
	type Running = Moments;

	fn window_contribution(&self, window_value: &f64) -> f64 {
		*window_value
	}

	fn combine_running(
		&self,
		group: &String,
		running: &Moments,
		newest_value: &f64,
		newest_coord: DateTime,
	) -> Option<TestOut> {
		TestVelocity {
			capacity: 3,
		}
		.combine_running(group, running, newest_value, newest_coord.to_order())
	}
}

impl RollingRegistration for SealedVelocity {
	const NAME: &'static str = "sealed_velocity_incremental";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(millis(120))
	}
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// This driver had no seal gate at all, so an operator declaring seal_after got
	// silence. A group that stops reporting must still have its state reclaimed, or
	// a high-cardinality group key grows without bound. Nothing arrives after the
	// initial batch here; the only thing that moves is the watermark.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<SealedVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "ETH", 0, 50.0))
			.build())
		.expect("apply");
	let before = h.snapshot_state().len();

	let fired = h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");

	assert!(fired > 0, "the insert must have armed a seal timer that the watermark then passes");
	assert!(
		h.snapshot_state().len() < before,
		"a fired seal timer must reclaim the meta of groups that stopped reporting, but the \
		 store went from {before} rows to {}",
		h.snapshot_state().len()
	);
}

#[test]
fn a_sealed_incremental_window_drops_a_mutation_for_a_sealed_coordinate() {
	// The gate has to refuse late mutations, not merely reclaim state: accepting one
	// would reopen a coordinate whose value has already been published as final.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<SealedVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 600, 10.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(600)).expect("advance watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");

	assert_eq!(out.diffs.len(), 0, "an insert into a sealed coordinate must be dropped");
}

#[test]
fn an_ungated_incremental_operator_arms_no_seal_timer() {
	// seal_after defaults to None. An operator that never opted into
	// sealing must not acquire a retention policy it did not ask for.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

	assert!(h.armed_timers().is_empty(), "an operator with seal_after = None must arm no timer");
}
