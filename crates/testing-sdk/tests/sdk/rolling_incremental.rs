// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{interface::catalog::flow::OperatorId, row::Row as CoreRow};
use reifydb_flow::window::accumulator::invertible::{LastValue, Moments};
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
use reifydb_value::{
	factory::time::millis,
	value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType},
};

// Velocity-style operator. The baseline is the running moments minus the newest window, which
// is what makes the score O(1) instead of a fold over the buffer.

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
	type Accumulator = LastValue<f64>;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn bucket_size(&self) -> Duration {
		millis(1)
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, f64)> {
		let group = row.utf8("group")?.to_string();
		let value = row.f64("value")?;
		Some((group, value))
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<DateTime, LastValue<f64>>) -> Option<TestOut> {
		// The reference fold the incremental path below has to reproduce exactly.
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
		_newest_coord: DateTime,
	) -> Option<TestOut> {
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

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self {
			capacity: 3,
		})
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}
}

fn input_shape() -> RowShape {
	RowShape::new(
		RowFamily::Deprecated,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("window_start", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Float8),
		],
	)
}

fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
		.with_shape(input_shape())
		.with_time(DateTime::from_millis(window_start))
		.build()
}

#[test]
fn baseline_excludes_newest_window() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	// The newest window must be excluded from its own baseline: mean(10, 20) = 15, not 30.
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
	// Emptying the buffer has to withdraw the previously emitted row; leaking a ghost row is
	// what breaks reorg retraction.
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
	// The update targets the newest window because a buried one would be dropped as late; the
	// running moments must swap old for new rather than accumulate both.
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
	// A fourth window evicts window 0, and the running moments have to drop it too or the
	// baseline keeps counting a value no longer in the buffer.
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

struct SealedVelocity;

impl RollingOperator for SealedVelocity {
	type GroupKey = String;
	type Accumulator = LastValue<f64>;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		3
	}

	fn bucket_size(&self) -> Duration {
		millis(1)
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(millis(120))
	}

	fn extract(&self, ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, f64)> {
		TestVelocity {
			capacity: 3,
		}
		.extract(ctx, row)
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<DateTime, LastValue<f64>>) -> Option<TestOut> {
		TestVelocity {
			capacity: 3,
		}
		.combine(group, buffer)
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
		.combine_running(group, running, newest_value, newest_coord)
	}
}

impl RollingRegistration for SealedVelocity {
	const NAME: &'static str = "sealed_velocity_incremental";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// A group that stops reporting must still be reclaimed, or a high-cardinality group key
	// grows without bound; nothing moves here after the initial batch except the watermark.
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
	// The gate has to refuse late mutations, not merely reclaim state: accepting one would
	// reopen a coordinate whose value was already published as final.
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
	// An operator that never opted into sealing must not acquire a retention policy.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

	assert!(h.armed_timers().is_empty(), "an operator with seal_after = None must arm no timer");
}
