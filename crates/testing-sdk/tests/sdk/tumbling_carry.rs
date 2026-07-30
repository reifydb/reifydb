// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::encoded::EncodedKey,
};
use reifydb_core::{interface::catalog::flow::FlowNodeId, metrics::heap::HeapSize, row::Row as CoreRow};
use reifydb_flow::window::{
	accumulator::invertible::RetainedAccumulator,
	span::{WindowCoord, WindowSpan},
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		FFIOperatorAdapter, column::operator::OperatorColumn, context::OperatorContext, view::RowView,
		windowed::tumbling_carry::*,
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestRowBuilder},
	harness::FFIOperatorHarnessBuilder,
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType};

// A TWAP-shaped fixture exercising the carry-forward rotation in
// isolation. Each window retains its observations (RetainedAccumulator keyed by
// timestamp); the value summed is incidental. `carry_in` echoes the
// prior window's closing observation so assertions can prove the carry
// rotated across the window boundary, not whether the integral math is
// right (that lives in the operator's own tests).

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct CarryOut {
	group: String,
	window_start: u64,
	sum: f64,
	carry_in: f64,
	has_carry: bool,
}

row!(CarryOut {
	group: String,
	window_start: u64,
	sum: f64,
	carry_in: f64,
	has_carry: bool
});

struct TestCarry;

impl TumblingCarryOperator for TestCarry {
	type GroupKey = String;
	type WindowSlot = u64;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;
	type Carry = f64;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let ts = row.u64("ts")?;
		let price = row.f64("price")?;
		Some((group, ts, (ts, price)))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_coord(coord, 60)
	}

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<u64>,
		value: &BTreeMap<u64, f64>,
		prev_carry: Option<&f64>,
	) -> Option<CarryOut> {
		(!value.is_empty()).then(|| CarryOut {
			group: group.clone(),
			window_start: span.start,
			sum: value.values().sum(),
			carry_in: prev_carry.copied().unwrap_or(0.0),
			has_carry: prev_carry.is_some(),
		})
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
		value.last_key_value().map(|(_, v)| *v)
	}
}

impl TumblingCarryRegistration for TestCarry {
	const NAME: &'static str = "test_carry";
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
		RowShapeField::unconstrained("ts", ValueType::Uint8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	])
}

fn input_row(rn: u64, group: &str, ts: u64, price: f64) -> CoreRow {
	TestRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(price)])
		.with_shape(input_shape())
		.build()
}

#[test]
fn first_window_has_no_carry() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 20.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(0));
	assert_eq!(r.f64("sum"), Some(30.0));
	assert_eq!(r.bool("has_carry"), Some(false), "first window has no prior close to carry in");
	assert_eq!(r.f64("carry_in"), Some(0.0));
}

#[test]
fn remove_empties_window_emits_remove() {
	// Removing the only observation empties the window's accumulator, so the
	// carry driver must withdraw the previously emitted row (terminal Remove
	// carrying the prior output) rather than leak a ghost row - required for
	// reorg-retraction correctness of the carry/EMA-family views.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(0));
	assert_eq!(r.f64("sum"), Some(10.0));
}

#[test]
fn second_window_carries_in_prior_window_close() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	// Window [0,60): closing observation (largest ts) is price 20.
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 20.0))
			.build())
		.expect("apply");
	// Window [60,120) opens: its carry_in must be the prior close, 20.
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 70, 5.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(60));
	assert_eq!(r.f64("sum"), Some(5.0));
	assert_eq!(r.bool("has_carry"), Some(true));
	assert_eq!(r.f64("carry_in"), Some(20.0), "carry rotated from the closed window's last observation");
}

#[test]
fn carry_rotates_across_three_windows_in_one_batch() {
	// Windows in a single batch must rotate the carry in window order:
	// w0 closes at 10 -> w60 carries 10 (closes at 20) -> w120 carries 20.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 60, 20.0))
			.insert(input_row(3, "BTC", 120, 30.0))
			.build())
		.expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 3);
	let w0 = post.row_ref(0).expect("r0");
	assert_eq!(w0.u64("window_start"), Some(0));
	assert_eq!(w0.bool("has_carry"), Some(false));
	let w60 = post.row_ref(1).expect("r1");
	assert_eq!(w60.u64("window_start"), Some(60));
	assert_eq!(w60.f64("carry_in"), Some(10.0));
	let w120 = post.row_ref(2).expect("r2");
	assert_eq!(w120.u64("window_start"), Some(120));
	assert_eq!(w120.f64("carry_in"), Some(20.0));
}

#[test]
fn update_in_current_window_recomputes_carry() {
	// The carry is derived from the (delta-correct) window value, so an
	// Update that changes the closing observation must change what the
	// next window carries in.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	// Update the still-open window's observation 10 -> 50.
	let _ = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 50.0))
			.build())
		.expect("apply");
	// New window carries in the updated close, 50 - not the original 10.
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 60, 1.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(60));
	assert_eq!(r.f64("carry_in"), Some(50.0), "carry reflects the post-update close");
}

#[test]
fn late_event_accepted_without_sealing() {
	// The carry driver carries no seal envelope in this fixture, so under
	// grace semantics there is no gate: a late event for the earlier window
	// [0,60) is accepted and (re)opens that window. Bounding mutability is
	// the seal gate's job (opt-in via seal_after / sealed_up_to), not an
	// implicit high-water drop.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 20.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert!(!out.diffs.is_empty(), "ungated carry driver accepts late events");
}

fn millis(value: u64) -> Duration {
	Duration::from_milliseconds_const(value as i64)
}

// TestCarry with sealing enabled, over a DateTime coordinate.
struct SealedCarry;

impl TumblingCarryOperator for SealedCarry {
	type GroupKey = String;
	type WindowSlot = DateTime;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;
	type Carry = f64;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(String, DateTime, (u64, f64))> {
		let (group, ts, contribution) = TestCarry.extract(ctx, row)?;
		Some((group, DateTime::from_millis(ts), contribution))
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, millis(60))
	}

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
		prev_carry: Option<&f64>,
	) -> Option<CarryOut> {
		(!value.is_empty()).then(|| CarryOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			sum: value.values().sum(),
			carry_in: prev_carry.copied().unwrap_or(0.0),
			has_carry: prev_carry.is_some(),
		})
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, prev_carry: Option<&f64>) -> Option<f64> {
		TestCarry.carry_forward(value, prev_carry)
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(millis(120))
	}
}

impl TumblingCarryRegistration for SealedCarry {
	const NAME: &'static str = "sealed_carry";
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

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// Carry windows prune relative to the newest window a group has SEEN, so a group
	// that stops reporting freezes at whatever it last held. Reclaiming it has to be
	// driven by the watermark instead. Nothing arrives after the initial batch here.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<SealedCarry>>>::new()
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
fn an_ungated_carry_operator_arms_no_seal_timer() {
	// seal_after defaults to None. An operator that never opted into
	// sealing must not acquire a retention policy it did not ask for.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

	assert!(h.armed_timers().is_empty(), "an operator with seal_after = None must arm no timer");
}
