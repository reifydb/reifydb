// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::{
	flow::diff::DiffType,
	operator::capabilities::{OperatorCapability, from_bitmask},
};
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::encoded::EncodedKey,
};
use reifydb_core::{interface::catalog::flow::FlowNodeId, metrics::heap::HeapSize, row::Row as CoreRow};
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
	ffi::exports::create_descriptor,
	operator::{
		FFIOperatorAdapter, column::operator::OperatorColumn, context::OperatorContext, view::RowView,
		windowed::tumbling::*,
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestRowBuilder},
	harness::FFIOperatorHarnessBuilder,
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType};

#[test]
fn an_operator_that_declares_reclaim_reaches_the_host_with_it() {
	// A group-scoped operator declares Reclaim in its own capability list, and that list is
	// the whole truth the host loads. If the bit were lost on the way into the descriptor,
	// reclaim_flow would skip the node and count it perpetual while its state grew - the leak
	// this work item closes, and invisible because the operator's source would still look
	// correct. TestVolume mirrors the 46 chaindex aggregators that now declare it.
	assert!(TestVolume::CAPABILITIES.contains(&OperatorCapability::Reclaim));

	let descriptor = create_descriptor::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>();

	assert!(
		from_bitmask(descriptor.capabilities).contains(&OperatorCapability::Reclaim),
		"a declared Reclaim must survive the descriptor round trip"
	);
}

// An invertible volume aggregator. Its accumulator keeps only running
// Moments (no per-slot map): Insert adds, Update is routed by the driver
// as remove(pre)+add(post), Remove subtracts. This is the case the old
// per-slot map existed to handle and that the pre/post diff now subsumes.

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
	type WindowSlot = u64;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
		let group = row.utf8("group")?.to_string();
		let slot = row.u64("slot")?;
		let size = row.f64("size")?;
		Some((group, slot, size))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_coord(coord, 60)
	}

	fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start,
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
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start).build()
	}
}

fn millis(value: u64) -> Duration {
	Duration::from_milliseconds_const(value as i64)
}

// TestVolume with sealing enabled: 60ms windows + 60ms grace, so windows seal once the
// frontier moves more than 120ms past their start. The coordinate is a DateTime rather than a
// bare u64 because the frontier is built from the seal ledger and the flow watermark, both of
// which are instants - a u64 coordinate carries no unit that either can be compared against.
#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default)]
struct SealedVolume;

impl TumblingOperator for SealedVolume {
	type GroupKey = String;
	type WindowSlot = DateTime;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn extract(&self, ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, DateTime, f64)> {
		let (group, slot, size) = TestVolume.extract(ctx, row)?;
		Some((group, DateTime::from_millis(slot), size))
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

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start.to_order()).build()
	}
}

// A removal-safe minimum aggregator over an ordered multiset. Demonstrates
// the non-invertible family: an Update that replaces the current minimum
// with a larger value must raise the window minimum, which a scalar
// running-min could not do.

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
	type WindowSlot = u64;
	type Accumulator = MinAccumulator;
	type Output = MinOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, OrdF64)> {
		let group = row.utf8("group")?.to_string();
		let slot = row.u64("slot")?;
		let size = row.f64("size")?;
		Some((group, slot, OrdF64::new(size)?))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_coord(coord, 60)
	}

	fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<MinOut> {
		Some(MinOut {
			group: group.clone(),
			window_start: span.start,
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

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start).build()
	}
}

fn input_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("slot", ValueType::Uint8),
		RowShapeField::unconstrained("size", ValueType::Float8),
	])
}

fn input_row(rn: u64, group: &str, slot: u64, size: f64) -> CoreRow {
	TestRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(slot), Value::float8(size)])
		.with_shape(input_shape())
		.build()
}

#[test]
fn single_insert_emits_insert() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// The crux of the redesign: an Update carries pre=10, post=25.
	// The driver routes it as remove(10)+add(25) on a running sum,
	// yielding 25 - not 10 + 25 = 35 - with NO per-slot map.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// Two distinct slots in one window sum to 15; a Remove carrying
	// pre=5 subtracts that contribution, leaving 10. No slot key is
	// needed - the diff's pre value is what gets subtracted.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// An emptied window emits a Remove of its previously emitted aggregate
	// row, so a downstream consumer withdraws the stale row instead of
	// leaking it. The accumulator is empty (finalize returns None); the
	// engine carries the prior value so the driver can emit the Remove.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// Grace semantics: SealedVolume seals windows whose start falls more
	// than seal_after (window 60 + grace 60 = 120) behind the routed
	// watermark. Advancing to window 180 seals window 0; a late insert for
	// it must be dropped. An ungated driver (TestVolume) accepts the same
	// late insert - covered by late_event_without_sealing_is_accepted.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(180)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0, "insert into a sealed window must be dropped");
}

#[test]
fn late_event_within_grace_is_accepted() {
	// Window 0 stays mutable while the watermark has not passed start + seal_after: with the
	// watermark at 120 (== 0 + 120), the boundary is inclusive on the mutable side. The watermark
	// has to be advanced explicitly, because arrival no longer moves the frontier - without it the
	// frontier sits at the epoch and this would assert acceptance against a gate that never closes,
	// which is true for any boundary rule and so tests none of them.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
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
	// The state the seal-on-timer design introduced: the frontier is the seal ledger merged with
	// the flow watermark, so a gated driver whose flow has not reported progress has nothing to
	// measure lateness against and must accept the row. Arrival alone used to advance the frontier,
	// which is what made a guest window with a stopped feed never seal. If this ever starts
	// dropping, the frontier is being derived from the batch again.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
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
	// With seal_after = None (the default) there is no gate: drivers accept
	// arbitrarily late mutations and state lives until the operator TTL.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "ungated drivers accept late inserts");
}

#[test]
fn remove_within_grace_is_applied_and_sealed_remove_is_dropped() {
	// Grace is the single mutability horizon for every mutation kind: a
	// retraction (reorg correction) is honored while the window is open or
	// within grace, and dropped once the window seals - the sealed value is
	// final by contract. Window 0 holds 15; a remove at watermark 60 (well
	// inside start + seal_after = 120) subtracts, leaving 10. Advancing the
	// watermark to 240 seals window 0 and reclaims its state; a further
	// remove is dropped and emits nothing, leaving the last published value
	// untouched.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
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
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// The removal-safe multiset case: window holds {5, 8, 6}, min = 5.
	// An Update replacing the 5 with 10 must raise the min to 6. A
	// running scalar min cannot do this; the multiset remove(5)+add(10)
	// leaves {6, 8, 10}.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
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
	// Sealing must reclaim the sealed window's accumulator state, not
	// just gate its mutations: state left behind is only reaped by the
	// wall-clock operator-state TTL backstop, which the paced jupiter
	// replay showed retaining hours of sealed windows. Window 0 is
	// created, then an insert at 240 moves the watermark so the seal
	// horizon (240 - 120) passes window 0: at least one of its store
	// keys must be gone afterwards.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let before = h.snapshot_state();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(240)).expect("advance watermark");
	let after = h.snapshot_state();
	let freed = before.keys().filter(|k| !after.contains_key(*k)).count();
	assert!(freed > 0, "sealing window 0 must remove its accumulator state from the store");

	// Control: without seal_after, ordinary apply churn must never
	// remove a key - reclamation may only come from the seal sweep.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
	// Two events share value 5. Removing one occurrence must keep the
	// min at 5 (the multiset still holds one 5).
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
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
