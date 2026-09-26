// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{OperatorClass, WindowKind, WindowRequirements, WindowSize, WindowSizeDomain},
	interface::{
		catalog::flow::OperatorId,
		flow::{OperatorCapability, from_bitmask},
	},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
	state::timer::TimerKind,
};
use reifydb_flow_async::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::{
			WindowAccumulator,
			invertible::{moments::Moments, multiset::Multiset, ordf64::OrdF64},
		},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::{exports::create_descriptor, operator::ExternCOperatorAdapter},
		view::RowView,
		windowed::{
			operator::{Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::{ExternCOperatorHarness, ExternCOperatorHarnessBuilder},
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{Value, datetime::DateTime, diff_type::DiffType, value_type::ValueType},
};

#[test]
fn a_declared_capability_reaches_the_host_through_the_descriptor() {
	// The descriptor's capability list is the whole truth the host loads: losing a bit there
	// silently gates the wrong methods while the operator's source still looks correct.
	assert!(TestVolume::CAPABILITIES.contains(&OperatorCapability::Delete));

	let descriptor = create_descriptor::<PlainDriver<TestVolume>>();

	assert!(
		from_bitmask(descriptor.capabilities).contains(&OperatorCapability::Delete),
		"a declared capability must survive the descriptor round trip"
	);
}

#[test]
fn a_windowed_driver_descriptor_carries_its_class_and_window_and_no_reason() {
	// The host reads only these fields: one left at its default makes CREATE check the wrong class or window.
	let descriptor = create_descriptor::<PlainDriver<TestVolume>>();

	assert_eq!(OperatorClass::from_u8(descriptor.class), Some(OperatorClass::Windowed));
	assert!(descriptor.unmanaged_because.ptr.is_null());
	assert_eq!(descriptor.window.takes_window, 1);
	assert_eq!(
		WindowRequirements::kinds_from_bitmask(descriptor.window.kinds),
		Some(&["tumbling", "sliding", "session"][..])
	);
	assert_eq!(WindowSizeDomain::from_u8(descriptor.window.domain), Some(WindowSizeDomain::Time));
	assert_eq!(descriptor.window.needs_pane, 0);
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

impl OperatorMetadata for TestVolume {
	const NAME: &'static str = "test_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestVolume {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
		Ok(row.row_time())
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Result<Option<(String, f64)>> {
		let (Some(group), Some(size)) = (row.utf8("group")?, row.f64("size")?) else {
			return Ok(None);
		};
		Ok(Some((group.to_string(), size)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> VolumeAccumulator {
		VolumeAccumulator::default()
	}
}

impl Emit for TestVolume {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			volume: value.get(),
		})
	}
}

// Sealing variant: 60ms windows plus 60ms lateness. Identical to TestVolume except for the seal
// envelope, so any difference in what these tests observe comes from sealing alone.
#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default)]
struct SealedVolume;

impl OperatorMetadata for SealedVolume {
	const NAME: &'static str = "sealed_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for SealedVolume {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
		Ok(row.row_time())
	}

	fn extract(&self, ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Result<Option<(String, f64)>> {
		TestVolume.extract(ctx, row)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> VolumeAccumulator {
		VolumeAccumulator::default()
	}
}

impl Emit for SealedVolume {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			volume: value.get(),
		})
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

impl OperatorMetadata for TestMin {
	const NAME: &'static str = "test_min";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestMin {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = MinAccumulator;
	type Output = MinOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
		Ok(row.row_time())
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		row: &impl RowView,
	) -> Result<Option<(String, OrdF64)>> {
		let (Some(group), Some(size)) = (row.utf8("group")?, row.f64("size")?) else {
			return Ok(None);
		};
		let Some(size) = OrdF64::new(size) else {
			return Ok(None);
		};
		Ok(Some((group.to_string(), size)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> MinAccumulator {
		MinAccumulator::default()
	}
}

impl Emit for TestMin {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OrdF64) -> Option<MinOut> {
		Some(MinOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			min: value.get(),
		})
	}
}

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("slot", ValueType::Uint8),
		RowShapeField::unconstrained("size", ValueType::Float8),
	]
}

fn window_order(millis: u64) -> u64 {
	DateTime::from_millis(millis as i64).to_order()
}

fn input_row(rn: u64, group: &str, slot: u64, size: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(slot), Value::float8(size)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(slot as i64))
		.build()
}

fn window_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
		retention: None,
		throttle: None,
	}
}

fn sealed_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: Some(WithSpan::Duration(millis(60))),
		immutable: None,
		retention: None,
		throttle: None,
	}
}

#[test]
fn single_insert_emits_insert() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Insert);
	let r = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
	assert_eq!(r.u64("window_start"), Some(window_order(0)));
	assert_eq!(r.f64("volume"), Some(10.0));
}

#[test]
fn update_applies_post_minus_pre_no_double_count() {
	// An update routed as remove(pre)+add(post) lands on 25; folding only post would give 35.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
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
fn an_update_carries_the_published_row_as_its_pre() {
	// A pre equal to the post makes every downstream consumer retract the new value instead of the old one.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let pre = diff.pre().expect("pre").row_ref(0).expect("r0");
	assert_eq!(pre.f64("volume"), Some(10.0));
	let post = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(post.f64("volume"), Some(15.0));
}

#[test]
fn a_second_update_carries_the_row_of_the_first_update_as_its_pre() {
	// A pre frozen at the insert row would retract a value downstream no longer holds.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 40, 1.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	let diff = &out.diffs[0];
	assert_eq!(diff.kind(), DiffType::Update);
	let pre = diff.pre().expect("pre").row_ref(0).expect("r0");
	assert_eq!(pre.f64("volume"), Some(15.0));
	let post = diff.post().expect("post").row_ref(0).expect("r0");
	assert_eq!(post.f64("volume"), Some(16.0));
}

#[test]
fn boundary_slot_belongs_to_next_window() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
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
	assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(window_order(0)));
	assert_eq!(post.row_ref(1).expect("r1").u64("window_start"), Some(window_order(60)));
}

#[test]
fn late_event_for_sealed_window_dropped() {
	// A window seals once the watermark passes start + lateness, and a sealed window must
	// refuse further inserts rather than reopen.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<SealedVolume>>>::new()
		.with(sealed_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(180)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0, "insert into a sealed window must be dropped");
}

#[test]
fn late_event_within_seal_is_accepted() {
	// The boundary is inclusive on the mutable side: at watermark == start + lateness the
	// window is still open. The watermark must be advanced explicitly, or the gate never
	// closes and the assertion would hold under any boundary rule.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<SealedVolume>>>::new()
		.with(sealed_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 120, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "window 0 is still within its lateness at watermark 120");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(99.0));
}

#[test]
fn a_gated_driver_admits_a_late_event_while_the_watermark_has_not_moved() {
	// The frontier comes from the seal ledger and the flow watermark, not from arrivals, so a
	// flow that has reported no progress has nothing to measure lateness against. If this ever
	// starts dropping, the frontier is being derived from the batch again.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<SealedVolume>>>::new()
		.with(sealed_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");

	assert_eq!(out.diffs.len(), 1, "with no watermark reported, an arbitrarily old window is still open");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(window_order(0)));
}

#[test]
fn late_event_while_lateness_is_open_is_accepted() {
	// while the lateness window has not elapsed, a driver must accept an arbitrarily late mutation
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "ungated drivers accept late inserts");
}

#[test]
fn remove_within_seal_is_applied_and_sealed_remove_is_dropped() {
	// Grace is the single mutability horizon for every mutation kind, retractions included: a
	// remove is honored while the window is open and dropped once it seals, because the sealed
	// value is final by contract.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<SealedVolume>>>::new()
		.with(sealed_with())
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
	assert_eq!(out.diffs.len(), 1, "retraction within the lateness must be honored");
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestMin>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<SealedVolume>>>::new()
		.with(sealed_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let before = h.snapshot_state();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
	let after = h.snapshot_state();
	assert!(
		before.keys().all(|k| after.contains_key(k)),
		"a window whose lateness has not passed must not reclaim any state"
	);
}

#[test]
fn min_remove_duplicate_keeps_value_until_last_removed() {
	// Removing one of two equal values must not evict the value itself from the multiset.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestMin>>>::new()
		.with(window_with())
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

#[test]
fn create_without_a_window_reports_flow_065() {
	// require_window must refuse a missing window before any row reaches the aggregator
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(ApplyWith::default())
		.build()
	else {
		panic!("create must refuse a missing window");
	};
	assert!(err.to_string().contains("FLOW_065"), "expected FLOW_065, got: {err}");
}

#[test]
fn create_with_the_wrong_window_kind_reports_flow_066() {
	// require_window must refuse a window kind this driver does not support
	let with = ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: None,
		}),
		lateness: None,
		immutable: None,
		retention: None,
		throttle: None,
	};
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(with)
		.build()
	else {
		panic!("create must refuse an unsupported window kind");
	};
	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}

fn throttled_harness() -> ExternCOperatorHarness<ExternCOperatorAdapter<PlainDriver<TestVolume>>> {
	ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<TestVolume>>>::new()
		.with(ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(millis(60_000)),
			}),
			lateness: None,
			immutable: None,
			retention: None,
			throttle: Some(millis(10_000)),
		})
		.build()
		.expect("harness")
}

fn only_update(out: &reifydb_core::interface::change::Change) -> (f64, f64) {
	let updates: Vec<_> = out.diffs.iter().filter(|d| d.kind() == DiffType::Update).collect();
	assert_eq!(updates.len(), 1, "exactly one update diff");
	assert_eq!(updates[0].post().expect("post").row_count(), 1, "exactly one updated row");
	let pre = updates[0].pre().expect("pre").row_ref(0).expect("r0").f64("volume").expect("pre volume");
	let post = updates[0].post().expect("post").row_ref(0).expect("r0").f64("volume").expect("post volume");
	(pre, post)
}

#[test]
fn a_new_throttled_window_publishes_its_first_row_at_once() {
	// A new window held back by the throttle would stay invisible downstream for a whole throttle.
	let mut h = throttled_harness();
	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 1_000, 7.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "a second new window inside the throttle still publishes at once");
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	assert_eq!(out.diffs[0].post().expect("post").row_ref(0).expect("r0").f64("volume"), Some(7.0));
}

#[test]
fn an_update_inside_the_throttle_publishes_nothing() {
	// Publishing every batch is the cost the throttle exists to remove.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0);
}

#[test]
fn an_update_past_the_throttle_publishes_every_entry() {
	// The throttle boundary is inclusive: at exactly last publish + throttle the window is due.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 10_000, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_late_fix_is_timed_by_the_frontier() {
	// Timing a late row by its own coordinate would hold a due window back until newer rows arrive.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 5_000, 5.0)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_window_emptied_inside_the_throttle_publishes_its_removal() {
	// A held-back removal leaves a row downstream for a window that no longer exists.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.remove(input_row(1, "BTC", 0, 10.0))
			.remove(input_row(2, "BTC", 1_000, 5.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let pre = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(pre.f64("volume"), Some(10.0), "the removal retracts the row downstream holds");
}

#[test]
fn a_dirty_window_publishes_once_more_when_it_closes() {
	// A window closing with unpublished changes would leave its final value unseen forever.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "ETH", 120_000, 1.0)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_window_published_when_due_is_not_republished_on_close() {
	// A due publish that leaves the window dirty makes the close emit the same row a second time.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 10_000, 1.0)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 16.0));
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(4, "ETH", 120_000, 1.0)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}

#[test]
fn a_clean_window_is_not_republished_on_close() {
	// Republishing an unchanged window on close doubles the output rows for nothing.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 120_000, 1.0)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}

#[test]
fn a_timer_close_publishes_the_dirty_window() {
	// A flow that goes quiet closes windows only on the timer, so the timer must publish too.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	let out = h
		.on_timer(DateTime::from_millis(120_000), TimerKind::Seal, b"")
		.expect("timer")
		.expect("the close publishes");
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_throttled_update_carries_the_last_published_row_not_the_skipped_one() {
	// Downstream never saw the skipped row, so retracting it would corrupt every consumer.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 10_000, 1.0)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 16.0));
}

#[test]
fn a_refilled_window_publishes_an_insert() {
	// Downstream already dropped the removed row, so an update retracting it corrupts every consumer.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "precondition: the emptied window publishes its removal");
	assert_eq!(out.diffs[0].kind(), DiffType::Remove, "precondition");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "a refilled window publishes at once, like any new window");
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	assert_eq!(out.diffs[0].post().expect("post").row_ref(0).expect("r0").f64("volume"), Some(5.0));
}

#[test]
fn an_emptied_dirty_window_is_not_removed_again_on_close() {
	// A second removal of a row downstream already dropped is a retraction of nothing.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1_000, 5.0)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.remove(input_row(1, "BTC", 0, 10.0))
			.remove(input_row(2, "BTC", 1_000, 5.0))
			.build())
		.expect("apply");
	assert_eq!(out.diffs[0].kind(), DiffType::Remove, "precondition: the emptied window publishes its removal");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "ETH", 120_000, 1.0)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}
