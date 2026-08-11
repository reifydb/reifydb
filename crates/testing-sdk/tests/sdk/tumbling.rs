// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::{
	flow::diff::DiffType,
	operator::capabilities::{OperatorCapability, from_bitmask},
};
use reifydb_codec::{key::encoded::EncodedKey, row::shape::RowShapeField};
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::heap::HeapSize, row::Row as CoreRow};
use reifydb_flow::window::{
	accumulator::{
		WindowAccumulator,
		invertible::{Moments, Multiset, OrdF64},
	},
	span::{WindowCoord, WindowSpan},
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	extern_c::exports::create_descriptor,
	operator::{
		ExternCOperatorAdapter, column::operator::OperatorColumn, context::OperatorContext, view::RowView,
		windowed::tumbling::*,
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	factory::time::millis,
	value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType},
};

#[test]
fn a_declared_capability_reaches_the_host_through_the_descriptor() {
	// The descriptor's capability list is the whole truth the host loads: losing a bit there
	// silently gates the wrong methods while the operator's source still looks correct.
	assert!(TestVolume::CAPABILITIES.contains(&OperatorCapability::Delete));

	let descriptor = create_descriptor::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>();

	assert!(
		from_bitmask(descriptor.capabilities).contains(&OperatorCapability::Delete),
		"a declared capability must survive the descriptor round trip"
	);
}

// An invertible volume aggregator holding only running moments: the driver routes an Update
// as remove(pre)+add(post), so no per-slot map is needed to undo a contribution.

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
struct VolumeAccumulator {
	moments: Moments,
}

impl WindowAccumulator for VolumeAccumulator {
	type Contribution = f64;
	type Output = OrdF64;

	fn add(&mut self, contribution: &f64) {
		self.moments.add(*contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		self.moments.remove(*contribution);
	}

	fn finalize(&self) -> Option<OrdF64> {
		(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite"))
	}

	fn is_empty(&self) -> bool {
		self.moments.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq)]
struct VolumeOut {
	group: String,
	window_start: u64,
	volume: f64,
}

row!(VolumeOut {
	group: String,
	window_start: u64,
	volume: f64
});

struct TestVolume;

impl TumblingOperator for TestVolume {
	type GroupKey = String;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, f64)> {
		let group = row.utf8("group")?.to_string();
		let size = row.f64("size")?;
		Some((group, size))
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, millis(60))
	}

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			volume: value.get(),
		})
	}
}

impl TumblingRegistration for TestVolume {
	const NAME: &'static str = "test_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start.to_order()).build()
	}
}

// Sealing variant: 60ms windows plus 60ms grace. Identical to TestVolume except for the seal
// envelope, so any difference in what these tests observe comes from sealing alone.
#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default)]
struct SealedVolume;

impl TumblingOperator for SealedVolume {
	type GroupKey = String;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn extract(&self, ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, f64)> {
		TestVolume.extract(ctx, row)
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, millis(60))
	}

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			volume: value.get(),
		})
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(millis(120))
	}
}

impl TumblingRegistration for SealedVolume {
	const NAME: &'static str = "sealed_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start.to_order()).build()
	}
}

// The non-invertible family: an Update replacing the current minimum with a larger value has
// to raise the window minimum, which a scalar running-min cannot do.

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
struct MinAccumulator {
	values: Multiset<OrdF64>,
}

impl WindowAccumulator for MinAccumulator {
	type Contribution = OrdF64;
	type Output = OrdF64;

	fn add(&mut self, contribution: &OrdF64) {
		self.values.add(*contribution);
	}

	fn remove(&mut self, contribution: &OrdF64) {
		self.values.remove(contribution);
	}

	fn finalize(&self) -> Option<OrdF64> {
		self.values.min().copied()
	}

	fn is_empty(&self) -> bool {
		self.values.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq)]
struct MinOut {
	group: String,
	window_start: u64,
	min: f64,
}

row!(MinOut {
	group: String,
	window_start: u64,
	min: f64
});

struct TestMin;

impl TumblingOperator for TestMin {
	type GroupKey = String;
	type Accumulator = MinAccumulator;
	type Output = MinOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, OrdF64)> {
		let group = row.utf8("group")?.to_string();
		let size = row.f64("size")?;
		Some((group, OrdF64::new(size)?))
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, millis(60))
	}

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: OrdF64) -> Option<MinOut> {
		Some(MinOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			min: value.get(),
		})
	}
}

impl TumblingRegistration for TestMin {
	const NAME: &'static str = "test_min";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start.to_order()).build()
	}
}

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("slot", ValueType::Uint8),
		RowShapeField::unconstrained("size", ValueType::Float8),
	]
}

fn input_row(rn: u64, group: &str, slot: u64, size: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(slot), Value::float8(size)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(slot))
		.build()
}

#[test]
fn single_insert_emits_insert() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Insert);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
	assert_eq!(r.u64("window_start"), Some(0));
	assert_eq!(r.f64("volume"), Some(10.0));
}

#[test]
fn update_applies_post_minus_pre_no_double_count() {
	// An update routed as remove(pre)+add(post) lands on 25; folding only post would give 35.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("volume"), Some(25.0));
}

#[test]
fn two_contributions_then_remove_subtracts_pre() {
	// The diff's pre value is what gets subtracted, so no per-slot key is needed to find the
	// contribution being withdrawn.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 5.0))
			.build())
		.expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("volume"), Some(10.0));
}

#[test]
fn remove_clears_window_emits_remove() {
	// The accumulator finalizes to nothing, so the prior value has to come from the engine for
	// the driver to withdraw the stale row instead of leaking it.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(r.f64("volume"), Some(10.0));
}

#[test]
fn boundary_slot_belongs_to_next_window() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 59, 1.0))
			.insert(input_row(2, "BTC", 60, 1.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 2);
	assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(0));
	assert_eq!(post.row_ref(1).expect("r1").u64("window_start"), Some(60));
}

#[test]
fn late_event_for_sealed_window_dropped() {
	// A window seals once the watermark passes start + seal_after, and a sealed window must
	// refuse further inserts rather than reopen.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(180)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0, "insert into a sealed window must be dropped");
}

#[test]
fn late_event_within_grace_is_accepted() {
	// The boundary is inclusive on the mutable side: at watermark == start + seal_after the
	// window is still open. The watermark must be advanced explicitly, or the gate never
	// closes and the assertion would hold under any boundary rule.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 120, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "window 0 is still within grace at watermark 120");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(99.0));
}

#[test]
fn a_gated_driver_admits_a_late_event_while_the_watermark_has_not_moved() {
	// The frontier comes from the seal ledger and the flow watermark, not from arrivals, so a
	// flow that has reported no progress has nothing to measure lateness against. If this ever
	// starts dropping, the frontier is being derived from the batch again.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");

	assert_eq!(out.diffs.len(), 1, "with no watermark reported, an arbitrarily old window is still open");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(0));
}

#[test]
fn late_event_without_sealing_is_accepted() {
	// Without a gate, drivers accept arbitrarily late mutations and state lives until the
	// operator TTL.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "ungated drivers accept late inserts");
}

#[test]
fn remove_within_grace_is_applied_and_sealed_remove_is_dropped() {
	// Grace is the single mutability horizon for every mutation kind, retractions included: a
	// remove is honored while the window is open and dropped once it seals, because the sealed
	// value is final by contract.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 5.0))
			.build())
		.expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 60, 1.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "retraction within grace must be honored");
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("volume"), Some(10.0));

	let _ = h.apply(TestChangeBuilder::new().insert(input_row(4, "BTC", 240, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(240)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0, "retraction of a sealed window must be dropped");
}

#[test]
fn multiple_groups_isolate_state() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(10.0));
	assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
	assert_eq!(post.row_ref(1).expect("r1").f64("volume"), Some(50.0));
}

#[test]
fn min_update_replacing_minimum_raises_window_min() {
	// Raising the minimum away is what a running scalar min cannot do; the multiset has to
	// surface the next-smallest value instead.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestMin>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 5.0))
			.insert(input_row(2, "BTC", 10, 8.0))
			.insert(input_row(3, "BTC", 20, 6.0))
			.build())
		.expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 5.0), input_row(1, "BTC", 0, 10.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("min"), Some(6.0));
}

#[test]
fn sealing_frees_window_state_from_the_store() {
	// Sealing has to reclaim the window's accumulator state, not just gate its mutations;
	// state left behind is only reaped by the wall-clock operator-state TTL backstop.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let before = h.snapshot_state();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(240)).expect("advance watermark");
	let after = h.snapshot_state();
	let freed = before.keys().filter(|k| !after.contains_key(*k)).count();
	assert!(freed > 0, "sealing window 0 must remove its accumulator state from the store");

	// Control: reclamation may only come from the seal sweep, never from ordinary apply churn.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let before = h.snapshot_state();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
	let after = h.snapshot_state();
	assert!(before.keys().all(|k| after.contains_key(k)), "an ungated driver must not reclaim any state");
}

#[test]
fn min_remove_duplicate_keeps_value_until_last_removed() {
	// Removing one of two equal values must not evict the value itself from the multiset.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TumblingDriver<TestMin>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 5.0))
			.insert(input_row(2, "BTC", 10, 5.0))
			.insert(input_row(3, "BTC", 20, 9.0))
			.build())
		.expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 5.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.f64("min"), Some(5.0), "one occurrence of 5 remains, min stays 5");
}
