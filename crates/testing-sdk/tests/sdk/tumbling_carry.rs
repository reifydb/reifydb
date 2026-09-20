// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{accumulator::invertible::retained_map::RetainedAccumulator, span::WindowSpan},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::RowView,
		windowed::{
			carry::CarryDriver,
			operator::{CarryEmit, WindowSettings, WindowedOperator},
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{Value, datetime::DateTime, diff_type::DiffType, value_type::ValueType},
};

// A TWAP-shaped fixture that isolates the carry rotation. `carry_in` echoes the prior
// window's closing observation, so assertions here are about the rotation and not the
// integral math, which the operator's own tests cover.

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

impl OperatorMetadata for TestCarry {
	const NAME: &'static str = "test_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestCarry {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let ts = row.u64("ts")?;
		let price = row.f64("price")?;
		Some((group, (ts, price)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for TestCarry {
	type Carry = f64;

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

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
		value.last_key_value().map(|(_, v)| *v)
	}
}

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("ts", ValueType::Uint8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	]
}

fn window_order(millis: u64) -> u64 {
	DateTime::from_millis(millis).to_order()
}

fn input_row(rn: u64, group: &str, ts: u64, price: f64) -> CoreRow {
	// The window coordinate IS #time now - the operator no longer returns one - so `ts` is
	// stamped as the row's time and kept as a column only because the accumulator keys its
	// retained observations by it. An unstamped row would sit at the epoch and every window
	// here would silently collapse into one bucket.
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(price)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(ts))
		.build()
}

fn window_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
	}
}

fn sealed_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: Some(WithSpan::Duration(millis(60))),
		immutable: None,
	}
}

#[test]
fn first_window_has_no_carry() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 20.0))
			.build())
		.expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(window_order(0)));
	assert_eq!(r.f64("sum"), Some(30.0));
	assert_eq!(r.bool("has_carry"), Some(false), "first window has no prior close to carry in");
	assert_eq!(r.f64("carry_in"), Some(0.0));
}

#[test]
fn remove_empties_window_emits_remove() {
	// Emptying a window has to withdraw the previously emitted row; leaking a ghost row is
	// what breaks reorg retraction.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Remove);
	let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(window_order(0)));
	assert_eq!(r.f64("sum"), Some(10.0));
}

#[test]
fn second_window_carries_in_prior_window_close() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	// The window closes on the largest ts, so 20 is what the next window must carry.
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 10.0))
			.insert(input_row(2, "BTC", 30, 20.0))
			.build())
		.expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 70, 5.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(window_order(60)));
	assert_eq!(r.f64("sum"), Some(5.0));
	assert_eq!(r.bool("has_carry"), Some(true));
	assert_eq!(r.f64("carry_in"), Some(20.0), "carry rotated from the closed window's last observation");
}

#[test]
fn carry_rotates_across_three_windows_in_one_batch() {
	// Windows opened in one batch must still rotate the carry in window order, not batch order.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
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
	assert_eq!(w0.u64("window_start"), Some(window_order(0)));
	assert_eq!(w0.bool("has_carry"), Some(false));
	let w60 = post.row_ref(1).expect("r1");
	assert_eq!(w60.u64("window_start"), Some(window_order(60)));
	assert_eq!(w60.f64("carry_in"), Some(10.0));
	let w120 = post.row_ref(2).expect("r2");
	assert_eq!(w120.u64("window_start"), Some(window_order(120)));
	assert_eq!(w120.f64("carry_in"), Some(20.0));
}

#[test]
fn update_in_current_window_recomputes_carry() {
	// The carry is derived from the window value, so an update to the closing observation must
	// change what the next window carries in.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	let _ = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 50.0))
			.build())
		.expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 60, 1.0)).build()).expect("apply");
	let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
	assert_eq!(r.u64("window_start"), Some(window_order(60)));
	assert_eq!(r.f64("carry_in"), Some(50.0), "carry reflects the post-update close");
}

#[test]
fn late_event_accepted_while_lateness_is_open() {
	// while the lateness window has not elapsed, a late event must still reopen its earlier window
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 20.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert!(!out.diffs.is_empty(), "ungated carry driver accepts late events");
}

struct SealedCarry;

impl OperatorMetadata for SealedCarry {
	const NAME: &'static str = "sealed_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for SealedCarry {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		TestCarry.extract(ctx, row)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for SealedCarry {
	type Carry = f64;

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
		prev_carry: Option<&f64>,
	) -> Option<CarryOut> {
		TestCarry.build_output(group, span, value, prev_carry)
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, prev_carry: Option<&f64>) -> Option<f64> {
		TestCarry.carry_forward(value, prev_carry)
	}
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// Carry windows prune relative to the newest window a group has seen, so a group that
	// stops reporting freezes; only the watermark can drive its reclamation.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<SealedCarry>>>::new()
		.with(sealed_with())
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
fn a_ladder_advancing_on_its_own_event_time_keeps_publishing_every_window() {
	// Production freeze this pins: solana::market::{twap,vwap,volume,ohlcv}::* published rows for
	// the very first block of a replayed corpus and then never again. The corpus carries event
	// timestamps but the watermark advanced on arrival, so from the second batch on the seal
	// horizon sat weeks ahead of every window and `seal` discarded each bucket before it emitted.
	//
	// The window coordinate and the seal horizon must be read on the same clock. Here the
	// watermark advances exactly as the feed does - from the rows' own #time, which is what
	// `max_input_time` feeds it in production - so a ladder that keeps receiving must keep
	// publishing, however many windows it has crossed.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<SealedCarry>>>::new()
		.with(sealed_with())
		.build()
		.expect("harness");

	let mut published = 0usize;
	for (rn, ts) in [(1u64, 0u64), (2, 60), (3, 120), (4, 180), (5, 240)] {
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(rn, "BTC", ts, 10.0 + ts as f64)).build())
			.expect("apply");
		assert!(
			!out.diffs.is_empty(),
			"the window at event_ts={ts} published nothing; a ladder whose watermark tracks its \
			 own feed must never seal the window it is currently filling"
		);
		published += 1;
		h.advance_watermark(DateTime::from_millis(ts)).expect("advance watermark");
	}

	assert_eq!(published, 5, "every window in the ladder must publish, not just the first");
}

#[test]
fn a_watermark_genuinely_past_the_seal_envelope_does_seal_the_window() {
	// The mirror of the freeze above, and the reason that one cannot simply be "never seal".
	// Sealing is what bounds operator state: once the watermark is truly past window + lateness,
	// late mutations for that window have to be refused, or a stalled group's buckets accumulate
	// without limit. SealedCarry seals 120ms after a 60ms window, so a watermark at 10_000ms is
	// far outside the envelope of the window starting at 0.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<SealedCarry>>>::new()
		.with(sealed_with())
		.build()
		.expect("harness");

	let opened = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
	assert!(!opened.diffs.is_empty(), "precondition: the window at 0 opened and published");

	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");

	let late = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
	assert!(
		late.diffs.is_empty(),
		"a row landing in a window the watermark has already sealed must be dropped, not merged; \
		 accepting it reopens a closed window and unbounds the operator's state"
	);
}

#[test]
fn a_carry_time_window_arms_a_seal_timer() {
	// a driver with a required window must always acquire a seal retention policy
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

	assert!(!h.armed_timers().is_empty(), "a windowed operator must arm a seal timer on its first insert");
}

#[test]
fn create_without_a_window_reports_flow_065() {
	// require_window must refuse a missing window before any row reaches the aggregator
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
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
	};
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<TestCarry>>>::new()
		.with(with)
		.build()
	else {
		panic!("create must refuse an unsupported window kind");
	};
	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}
