// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_flow_async::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::invertible::retained_map::RetainedAccumulator, coord::OrdinalCoord,
		settings::WindowSettings, span::WindowSpan,
	},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::{ColumnsView, RowView},
		windowed::{
			carry::CarryDriver,
			operator::{CarryEmit, WindowedOperator},
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	in_process::harness::{InProcessOperatorHarness, InProcessOperatorHarnessBuilder},
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{
		Value, column_view::FromColumnView, datetime::DateTime, system_columns::column_view,
		value_type::ValueType,
	},
};

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct LateOut {
	window_start: u64,
	sum: f64,
	carry_in: f64,
}

row!(LateOut {
	window_start: u64,
	sum: f64,
	carry_in: f64
});

struct LateCarry;

impl OperatorMetadata for LateCarry {
	const NAME: &'static str = "late_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for LateCarry {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = LateOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTime>>> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> Result<Option<DateTime>> { Ok(row.row_time()) })
					.transpose()
					.map(Option::flatten)
			})
			.collect()
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> Result<Option<(String, (u64, f64))>> {
						let (Some(group), Some(ts), Some(price)) =
							(row.utf8("group")?, row.u64("ts")?, row.f64("price")?)
						else {
							return Ok(None);
						};
						Ok(Some((group.to_string(), (ts, price))))
					})
					.transpose()
					.map(Option::flatten)
			})
			.collect()
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for LateCarry {
	type Carry = f64;

	fn build_output(
		&self,
		_group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
		prev_carry: Option<&f64>,
	) -> Option<LateOut> {
		(!value.is_empty()).then(|| LateOut {
			window_start: span.start.to_order(),
			sum: value.values().sum(),
			carry_in: prev_carry.copied().unwrap_or(0.0),
		})
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
		value.last_key_value().map(|(_, v)| *v)
	}
}

const WINDOW: u64 = 60_000;

fn with(size: u64, lateness: u64, immutable: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(size)),
		}),
		lateness: Some(WithSpan::Duration(millis(lateness))),
		immutable: Some(WithSpan::Duration(millis(immutable))),
		retention: None,
	}
}

fn idle_with(retention: Option<u64>) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(WINDOW)),
		}),
		lateness: Some(WithSpan::Duration(millis(60_000))),
		immutable: None,
		retention: retention.map(millis),
	}
}

fn insert(rn: u64, ts: u64, price: f64) -> Change {
	insert_group(rn, "BTC", ts, price)
}

fn insert_group(rn: u64, group: &str, ts: u64, price: f64) -> Change {
	let row = TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(price)])
		.with_fields(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Uint8),
			RowShapeField::unconstrained("price", ValueType::Float8),
		])
		.with_time(DateTime::from_millis(ts as i64))
		.build();
	TestChangeBuilder::new().insert(row).build()
}

fn read<T: FromColumnView>(batch: &RecordBatch, row: usize, name: &str) -> Option<T> {
	column_view(batch, name).unwrap().and_then(|column| column.get_as::<T>(row).unwrap())
}

fn published(out: &Change) -> BTreeMap<u64, (f64, f64)> {
	let mut rows = BTreeMap::new();
	for diff in out.diffs.iter() {
		let Some(post) = diff.post() else {
			continue;
		};
		for row in 0..post.num_rows() {
			let start = read::<u64>(post, row, "window_start").expect("window_start");
			let sum = read::<f64>(post, row, "sum").expect("sum");
			let carry_in = read::<f64>(post, row, "carry_in").expect("carry_in");
			rows.insert(start, (sum, carry_in));
		}
	}
	rows
}

fn window_order(millis: u64) -> u64 {
	DateTime::from_millis(millis as i64).to_order()
}

#[test]
fn a_late_row_inside_lateness_lands_in_a_window_older_than_immutable() {
	// The view admits any row above the lateness horizon, so folding a window on immutable alone silently loses it.
	let mut h = InProcessOperatorHarnessBuilder::<CarryDriver<LateCarry>>::new()
		.with(with(WINDOW, 60_000, 59_000))
		.build()
		.expect("harness");
	h.apply(insert(1, 0, 10.0)).expect("apply");
	h.apply(insert(2, 70_000, 5.0)).expect("apply");
	h.advance_watermark(DateTime::from_millis(70_000)).expect("advance watermark");

	let out = h.apply(insert(3, 50_000, 99.0)).expect("apply");

	let rows = published(&out);
	assert_eq!(
		rows.get(&window_order(0)).map(|(sum, _)| *sum),
		Some(109.0),
		"the late row is 20s inside lateness, so window 0 must take it and republish, got {rows:?}"
	);
	assert_eq!(
		rows.get(&window_order(WINDOW)).map(|(_, carry_in)| *carry_in),
		Some(99.0),
		"window 0's new close must carry into the live window after it, got {rows:?}"
	);
}

#[test]
fn a_carry_view_with_immutable_stays_bounded_while_the_watermark_tracks_the_feed() {
	// Admitting late rows must not unbound state: windows past the lateness horizon must still fold.
	let feed = |windows: u64| {
		let mut h = InProcessOperatorHarnessBuilder::<CarryDriver<LateCarry>>::new()
			.with(with(60, 60, 59))
			.build()
			.expect("harness");
		for i in 0..windows {
			let ts = i * 60;
			h.apply(insert(i + 1, ts, i as f64)).expect("apply");
			h.advance_watermark(DateTime::from_millis(ts as i64)).expect("advance watermark");
		}
		h.group_state().len()
	};

	let (short, long) = (feed(20), feed(40));

	assert!(
		long <= short + 2,
		"twice the windows must not mean more state: 20 windows left {short} rows, 40 left {long}"
	);
	assert!(long < 20, "40 windows past a one-window lateness must leave few rows, but left {long}");
}

fn idle_harness(retention: Option<u64>) -> InProcessOperatorHarness<CarryDriver<LateCarry>> {
	InProcessOperatorHarnessBuilder::<CarryDriver<LateCarry>>::new()
		.with(idle_with(retention))
		.build()
		.expect("harness")
}

fn carry_in_of(out: &Change, window: u64) -> Option<f64> {
	published(out).get(&window_order(window)).map(|(_, carry_in)| *carry_in)
}

#[test]
fn a_group_idle_past_window_and_lateness_carries_its_close_into_its_next_window() {
	// Dropping an idle group's meta loses its sealed carry, so a gap-filling operator would re-warm from nothing.
	let mut h = idle_harness(None);
	h.apply(insert(1, 0, 10.0)).expect("apply");
	h.advance_watermark(DateTime::from_millis(600_000)).expect("advance watermark");

	let out = h.apply(insert(2, 660_000, 5.0)).expect("apply");

	assert_eq!(
		carry_in_of(&out, 660_000),
		Some(10.0),
		"the window after the gap must carry in the close before it"
	);
}

#[test]
fn a_retention_drops_an_idle_group_only_once_it_is_idle_past_the_horizon_by_more_than_retention() {
	// Dropping at the horizon would lose the carry retention exists to keep, and never dropping would leak it.
	let mut within = idle_harness(Some(300_000));
	within.apply(insert(1, 0, 10.0)).expect("apply");
	within.advance_watermark(DateTime::from_millis(400_000)).expect("advance watermark");
	let kept = within.apply(insert(2, 420_000, 5.0)).expect("apply");

	let mut past = idle_harness(Some(300_000));
	past.apply(insert(1, 0, 10.0)).expect("apply");
	past.advance_watermark(DateTime::from_millis(600_000)).expect("advance watermark");
	let dropped = past.apply(insert(2, 660_000, 5.0)).expect("apply");

	assert_eq!(
		carry_in_of(&kept, 420_000),
		Some(10.0),
		"idle by less than the retention past the horizon keeps the carry"
	);
	assert_eq!(
		carry_in_of(&dropped, 660_000),
		Some(0.0),
		"idle by more than the retention past the horizon drops it"
	);
}

#[test]
fn a_retention_bounds_the_state_left_by_groups_that_went_idle() {
	// Without the retention drop every idle group keeps a meta forever, so state grows with groups seen.
	let feed = |groups: u64, retention: Option<u64>| {
		let mut h = idle_harness(retention);
		for g in 0..groups {
			h.apply(insert_group(g + 1, &format!("G{g}"), 0, 10.0)).expect("apply");
		}
		h.advance_watermark(DateTime::from_millis(3_600_000)).expect("advance watermark");
		h.snapshot_state().len()
	};

	let (short, long) = (feed(10, Some(300_000)), feed(40, Some(300_000)));
	let (kept_short, kept_long) = (feed(10, None), feed(40, None));

	assert_eq!(
		long, short,
		"with retention idle groups must leave nothing per group: 10 left {short}, 40 left {long}"
	);
	assert!(
		kept_long >= kept_short + 30,
		"without retention every idle group keeps its meta: 10 left {kept_short}, 40 left {kept_long}"
	);
}

struct SlotCarry;

impl OperatorMetadata for SlotCarry {
	const NAME: &'static str = "slot_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for SlotCarry {
	type Coord = OrdinalCoord;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = LateOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, _cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<OrdinalCoord>>> {
		Ok(vec![None; rows.len()])
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		_cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>> {
		Ok(vec![None; rows.len()])
	}

	fn new_accumulator(&self, _settings: &WindowSettings<OrdinalCoord>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for SlotCarry {
	type Carry = f64;

	fn build_output(
		&self,
		_group: &String,
		_span: WindowSpan<OrdinalCoord>,
		_value: &BTreeMap<u64, f64>,
		_prev_carry: Option<&f64>,
	) -> Option<LateOut> {
		None
	}

	fn carry_forward(&self, _value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
		None
	}
}

#[test]
fn a_slot_carry_refuses_a_retention_it_cannot_measure() {
	// A retention is a duration and a slot window has no clock, so accepting it would bound nothing.
	let with = ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Count(10),
		}),
		lateness: Some(WithSpan::Count(2)),
		immutable: None,
		retention: Some(millis(60_000)),
	};

	let Err(err) = InProcessOperatorHarnessBuilder::<CarryDriver<SlotCarry>>::new().with(with).build() else {
		panic!("a slot carry must refuse a retention");
	};

	assert!(err.to_string().contains("FLOW_080"), "expected FLOW_080, got: {err}");
}
