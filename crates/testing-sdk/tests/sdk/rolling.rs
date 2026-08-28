// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::shape::RowShapeField};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	row::Row as CoreRow,
};
use reifydb_flow::window::accumulator::{WindowAccumulator, invertible::moments::Moments};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		column::operator::OperatorColumn, context::GuestContext,
		extern_c::binding::operator::ExternCOperatorAdapter, view::RowView, windowed::rolling::*,
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	config::Config,
	factory::time::millis,
	value::{Value, datetime::DateTime, diff_type::DiffType, duration::Duration, value_type::ValueType},
};

// Rolling sum where each window is itself an invertible accumulator, so rows can share a
// window coordinate and a single event can be removed without dropping the whole window.

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
struct WindowSum {
	moments: Moments,
}

impl WindowAccumulator for WindowSum {
	type Contribution = f64;
	type Output = f64;

	fn add(&mut self, contribution: &f64) {
		self.moments.add(*contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		self.moments.remove(*contribution);
	}

	fn finalize(&self) -> Option<f64> {
		(!self.moments.is_empty()).then(|| self.moments.sum())
	}

	fn is_empty(&self) -> bool {
		self.moments.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq)]
struct TestOut {
	group: String,
	rolling_sum: f64,
	windows: u32,
}

row!(TestOut {
	group: String,
	rolling_sum: f64,
	windows: u32
});

struct TestRollingSum {
	capacity: usize,
}

impl RollingOperator for TestRollingSum {
	type GroupKey = String;

	type WindowSlot = DateTime;

	type Accumulator = WindowSum;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn bucket_size(&self) -> Duration {
		millis(1)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext, row: &impl RowView) -> Option<(String, f64)> {
		let group = row.utf8("group")?.to_string();
		let value = row.f64("value")?;
		Some((group, value))
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<DateTime, WindowSum>) -> Option<TestOut> {
		if buffer.is_empty() {
			return None;
		}
		let rolling_sum = buffer.values().filter_map(|w| w.finalize()).sum();
		Some(TestOut {
			group: group.clone(),
			rolling_sum,
			windows: buffer.len() as u32,
		})
	}
}

impl RollingRegistration for TestRollingSum {
	const NAME: &'static str = "test_rolling_sum";
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

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("value", ValueType::Float8),
	]
}

fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(window_start))
		.build()
}

#[test]
fn single_insert_emits_insert() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Insert);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(10.0));
	assert_eq!(r.u32("windows"), Some(1));
}

#[test]
fn multiple_events_accumulate_within_one_window() {
	// Two rows sharing a window coordinate must accumulate, not overwrite each other.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 3.0))
			.insert(input_row(2, "BTC", 0, 4.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(7.0));
	assert_eq!(r.u32("windows"), Some(1), "both rows landed in the same window");
}

#[test]
fn partial_remove_within_window_keeps_window_alive() {
	// Removing one of two events inside a window must leave the window standing, not drop it.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 3.0))
			.insert(input_row(2, "BTC", 0, 4.0))
			.build())
		.expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 3.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(4.0));
	assert_eq!(r.u32("windows"), Some(1), "window survives partial removal");
}

#[test]
fn update_within_window_applies_post_minus_pre() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(25.0), "25, not 10 + 25");
}

#[test]
fn buffer_fills_then_evicts_oldest_window() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 1.0))
			.insert(input_row(2, "BTC", 60, 2.0))
			.insert(input_row(3, "BTC", 120, 3.0))
			.insert(input_row(4, "BTC", 180, 4.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(9.0), "window 0 evicted: 2+3+4");
	assert_eq!(r.u32("windows"), Some(3));
}

#[test]
fn late_window_event_accepted_without_sealing() {
	// Without a lateness envelope there is no implicit high-water gate, so a late event merges
	// into its older coordinate; capacity eviction is what bounds this driver.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert!(!out.diffs.is_empty(), "ungated rolling driver accepts late events");
}

#[test]
fn remove_clears_buffer_emits_remove() {
	// Emptying the buffer has to withdraw the previously emitted row; leaking a ghost row is
	// what breaks reorg retraction.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(r.f64("rolling_sum"), Some(10.0));
}

#[test]
fn multiple_groups_isolate_buffers() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "ETH", 0, 50.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 2);
	assert_eq!(post.row_ref(0).expect("r0").utf8("group").as_deref(), Some("BTC"));
	assert_eq!(post.row_ref(0).expect("r0").f64("rolling_sum"), Some(10.0));
	assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
	assert_eq!(post.row_ref(1).expect("r1").f64("rolling_sum"), Some(50.0));
}

struct SealedRollingSum;

impl RollingOperator for SealedRollingSum {
	type GroupKey = String;

	type WindowSlot = DateTime;

	type Accumulator = WindowSum;
	type Output = TestOut;

	fn capacity(&self) -> usize {
		3
	}

	fn bucket_size(&self) -> Duration {
		millis(1)
	}

	fn lateness(&self) -> Option<Duration> {
		Some(millis(120))
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, ctx: &mut impl GuestContext, row: &impl RowView) -> Option<(String, f64)> {
		TestRollingSum {
			capacity: 3,
		}
		.extract(ctx, row)
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<DateTime, WindowSum>) -> Option<TestOut> {
		if buffer.is_empty() {
			return None;
		}
		Some(TestOut {
			group: group.clone(),
			rolling_sum: buffer.values().filter_map(|w| w.finalize()).sum(),
			windows: buffer.len() as u32,
		})
	}
}

impl RollingRegistration for SealedRollingSum {
	const NAME: &'static str = "sealed_rolling_sum";
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<SealedRollingSum>>>::new()
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
fn an_ungated_rolling_operator_arms_no_seal_timer() {
	// An operator that never opted into sealing must not acquire a retention policy.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

	assert!(h.armed_timers().is_empty(), "an operator with lateness = None must arm no timer");
}
