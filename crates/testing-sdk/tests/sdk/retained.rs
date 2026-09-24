// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	operator_with::ApplyWith,
	row::Row as CoreRow,
	state::timer::TimerKind,
};
use reifydb_flow::window::{
	accumulator::invertible::retained_map::RetainedAccumulator, settings::WindowSettings, span::WindowSpan,
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
			operator::{Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
			retained::RetainedDriver,
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
	value::{Value, datetime::DateTime, diff_type::DiffType, row_number::RowNumber, value_type::ValueType},
};

#[derive(Clone, Debug, PartialEq)]
struct RetainedOut {
	group: String,
	count: u64,
	volume: f64,
}

row!(RetainedOut {
	group: String,
	count: u64,
	volume: f64
});

struct TestRetained;

impl OperatorMetadata for TestRetained {
	const NAME: &'static str = "test_retained";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestRetained {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, i64>;
	type Output = RetainedOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, i64))> {
		let group = row.utf8("group")?.to_string();
		let key = row.u64("key")?;
		let value = row.i64("value")?;
		Some((group, (key, value)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, i64> {
		RetainedAccumulator::default()
	}
}

impl Emit for TestRetained {
	type Kinds = NoRolling;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, i64>,
	) -> Option<RetainedOut> {
		Some(RetainedOut {
			group: group.clone(),
			count: value.len() as u64,
			volume: value.values().sum::<i64>() as f64,
		})
	}
}

type Retained = ExternCOperatorAdapter<RetainedDriver<TestRetained, u64, i64>>;
type Plain = ExternCOperatorAdapter<PlainDriver<TestRetained>>;

fn input_row(rn: u64, group: &str, key: u64, at: u64, value: i64) -> CoreRow {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(key), Value::Int8(value)])
		.with_fields(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("key", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Int8),
		])
		.with_time(DateTime::from_millis(at))
		.build()
}

fn with(throttle: Option<u64>) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60_000)),
		}),
		lateness: None,
		immutable: None,
		retention: None,
		throttle: throttle.map(millis),
	}
}

fn throttled_harness() -> ExternCOperatorHarness<Retained> {
	ExternCOperatorHarnessBuilder::<Retained>::new().with(with(Some(10_000))).build().expect("harness")
}

fn only_update(out: &Change) -> (f64, f64) {
	let updates: Vec<_> = out.diffs.iter().filter(|d| d.kind() == DiffType::Update).collect();
	assert_eq!(updates.len(), 1, "exactly one update diff");
	assert_eq!(updates[0].post().expect("post").row_count(), 1, "exactly one updated row");
	let pre = updates[0].pre().expect("pre").row_ref(0).expect("r0").f64("volume").expect("pre volume");
	let post = updates[0].post().expect("post").row_ref(0).expect("r0").f64("volume").expect("post volume");
	(pre, post)
}

type Seen = Vec<(DiffType, Vec<(RowNumber, Vec<Value>)>, Vec<(RowNumber, Vec<Value>)>)>;

fn seen(out: &Change) -> Seen {
	let rows = |columns: Option<&reifydb_core::value::column::columns::Columns>| {
		columns.map_or_else(Vec::new, |columns| {
			(0..columns.row_count()).map(|i| (columns.row_numbers()[i], columns.row(i))).collect()
		})
	};
	out.diffs.iter().map(|d| (d.kind(), rows(d.pre()), rows(d.post()))).collect()
}

#[test]
fn parity_with_plain_driver_without_throttle() {
	// The retained driver replaces the plain one for retained operators, so any drift is a wrong row downstream.
	let mut plain = ExternCOperatorHarnessBuilder::<Plain>::new().with(with(None)).build().expect("harness");
	let mut retained = ExternCOperatorHarnessBuilder::<Retained>::new().with(with(None)).build().expect("harness");
	let batches: Vec<Change> = vec![
		TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 1, 0, 10))
			.insert(input_row(2, "ETH", 1, 1_000, 7))
			.build(),
		TestChangeBuilder::new().insert(input_row(3, "BTC", 2, 2_000, 5)).build(),
		TestChangeBuilder::new().update(input_row(1, "BTC", 1, 0, 10), input_row(1, "BTC", 1, 0, 12)).build(),
		TestChangeBuilder::new().insert(input_row(4, "BTC", 1, 3_000, 99)).build(),
		TestChangeBuilder::new().remove(input_row(1, "BTC", 1, 0, 12)).build(),
		TestChangeBuilder::new()
			.remove(input_row(4, "BTC", 1, 3_000, 99))
			.remove(input_row(3, "BTC", 2, 2_000, 5))
			.build(),
		TestChangeBuilder::new().insert(input_row(5, "BTC", 3, 4_000, 1)).build(),
	];
	for (step, batch) in batches.into_iter().enumerate() {
		let theirs = plain.apply(batch.clone()).expect("plain apply");
		let ours = retained.apply(batch).expect("retained apply");
		assert_eq!(seen(&ours), seen(&theirs), "step {step}");
	}
	plain.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	retained.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let close = TestChangeBuilder::new().insert(input_row(6, "ETH", 1, 120_000, 1)).build();
	let theirs = plain.apply(close.clone()).expect("plain apply");
	let ours = retained.apply(close).expect("retained apply");
	assert_eq!(seen(&ours), seen(&theirs), "close");
}

#[test]
fn a_new_throttled_window_publishes_its_first_row_at_once() {
	// A new window held back by the throttle would stay invisible downstream for a whole throttle.
	let mut h = throttled_harness();
	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 1, 1_000, 7)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "a second new window inside the throttle still publishes at once");
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	assert_eq!(out.diffs[0].post().expect("post").row_ref(0).expect("r0").f64("volume"), Some(7.0));
}

#[test]
fn an_update_inside_the_throttle_publishes_nothing() {
	// Publishing every batch is the cost the throttle exists to remove.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 0);
}

#[test]
fn an_update_past_the_throttle_publishes_every_entry() {
	// The throttle boundary is inclusive: at exactly last publish + throttle the window is due.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 10_000, 5)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1);
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_late_fix_is_timed_by_the_frontier() {
	// Timing a late row by its own coordinate would hold a due window back until newer rows arrive.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 5_000, 5)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_window_emptied_inside_the_throttle_publishes_its_removal() {
	// A held-back removal leaves a row downstream for a window that no longer exists.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.remove(input_row(1, "BTC", 1, 0, 10))
			.remove(input_row(2, "BTC", 2, 1_000, 5))
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
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "ETH", 1, 120_000, 1)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 15.0));
}

#[test]
fn a_window_published_when_due_is_not_republished_on_close() {
	// A due publish that leaves the window dirty makes the close emit the same row a second time.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 3, 10_000, 1)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 16.0));
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(4, "ETH", 1, 120_000, 1)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}

#[test]
fn a_clean_window_is_not_republished_on_close() {
	// Republishing an unchanged window on close doubles the output rows for nothing.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "ETH", 1, 120_000, 1)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}

#[test]
fn a_timer_close_publishes_the_dirty_window() {
	// A flow that goes quiet closes windows only on the timer, so the timer must publish too.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
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
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 3, 10_000, 1)).build()).expect("apply");
	assert_eq!(only_update(&out), (10.0, 16.0));
}

#[test]
fn a_refilled_window_publishes_an_insert() {
	// Downstream already dropped the removed row, so an update retracting it corrupts every consumer.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "precondition: the emptied window publishes its removal");
	assert_eq!(out.diffs[0].kind(), DiffType::Remove, "precondition");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	assert_eq!(out.diffs.len(), 1, "a refilled window publishes at once, like any new window");
	assert_eq!(out.diffs[0].kind(), DiffType::Insert);
	assert_eq!(out.diffs[0].post().expect("post").row_ref(0).expect("r0").f64("volume"), Some(5.0));
}

#[test]
fn an_emptied_dirty_window_is_not_removed_again_on_close() {
	// A second removal of a row downstream already dropped is a retraction of nothing.
	let mut h = throttled_harness();
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	let out = h
		.apply(TestChangeBuilder::new()
			.remove(input_row(1, "BTC", 1, 0, 10))
			.remove(input_row(2, "BTC", 2, 1_000, 5))
			.build())
		.expect("apply");
	assert_eq!(out.diffs[0].kind(), DiffType::Remove, "precondition: the emptied window publishes its removal");
	h.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "ETH", 1, 120_000, 1)).build()).expect("apply");
	assert!(out.diffs.iter().all(|d| d.kind() == DiffType::Insert), "only the new window publishes");
}
