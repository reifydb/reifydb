// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
	state::timer::TimerKind,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		settings::WindowSettings,
	},
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
			operator::{AllKinds, Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
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

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, PartialEq, HeapSize)]
pub(crate) struct PaneSum {
	sum: f64,
	count: u64,
}

impl WindowAccumulator for PaneSum {
	type Contribution = f64;
	type Output = f64;

	fn add(&mut self, contribution: &f64) {
		self.sum += contribution;
		self.count += 1;
	}

	fn remove(&mut self, contribution: &f64) {
		self.sum -= contribution;
		self.count = self.count.saturating_sub(1);
	}

	fn finalize(&self) -> Option<f64> {
		(self.count > 0).then_some(self.sum)
	}

	fn is_empty(&self) -> bool {
		self.count == 0
	}
}

impl MergeAccumulator for PaneSum {
	fn merge(&mut self, other: &Self) {
		self.sum += other.sum;
		self.count += other.count;
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SumOut {
	group: String,
	sum: f64,
	start: u64,
	end: u64,
}

row!(SumOut {
	group: String,
	sum: f64,
	start: u64,
	end: u64
});

macro_rules! sum_operator {
	($name:ident, $kinds:ty) => {
		pub(crate) struct $name;

		impl OperatorMetadata for $name {
			const NAME: &'static str = "sum";
			const VERSION: &'static str = "0.0.1";
			const DESCRIPTION: &'static str = "test fixture";
			const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
			const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
			const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
		}

		impl WindowedOperator for $name {
			type Coord = DateTime;
			type GroupKey = String;
			type Accumulator = PaneSum;
			type Output = SumOut;

			fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
				Ok(Self)
			}

			fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
				Ok(row.row_time())
			}

			fn extract(
				&self,
				_: &mut impl GuestContext<Windowed>,
				row: &impl RowView,
			) -> Result<Option<(String, f64)>> {
				let (Some(group), Some(value)) = (row.utf8("group")?, row.f64("value")?) else {
					return Ok(None);
				};
				Ok(Some((group.to_string(), value)))
			}

			fn new_accumulator(&self, _: &WindowSettings<DateTime>) -> PaneSum {
				PaneSum::default()
			}
		}

		impl Emit for $name {
			type Kinds = $kinds;

			fn build_output(
				&self,
				group: &String,
				span: reifydb_flow::window::span::WindowSpan<DateTime>,
				value: &f64,
			) -> Option<SumOut> {
				Some(SumOut {
					group: group.clone(),
					sum: *value,
					start: span.start.to_order(),
					end: span.end.to_order(),
				})
			}
		}
	};
}

sum_operator!(SumAnyKind, AllKinds);
sum_operator!(SumTumblingOnly, NoRolling);

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("ts", ValueType::Uint8),
		RowShapeField::unconstrained("value", ValueType::Float8),
	]
}

pub(crate) fn input_row(rn: u64, group: &str, ts: u64, value: f64) -> CoreRow {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(value)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(ts))
		.build()
}

fn rolling(size: u64, pane: Option<u64>, lateness: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(size)),
			lag: None,
			pane: pane.map(millis),
		}),
		lateness: Some(WithSpan::Duration(millis(lateness))),
		immutable: None,
		retention: None,
	}
}

fn tumbling(size: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(size)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
		retention: None,
	}
}

type Emitted = Vec<(DiffType, f64, u64, u64)>;

pub(crate) fn render(out: &Change) -> Emitted {
	let mut rendered = Vec::new();
	for diff in &out.diffs {
		let rows = match diff.kind() {
			DiffType::Remove => diff.pre().expect("pre"),
			_ => diff.post().expect("post"),
		};
		for i in 0..rows.row_count() {
			let r = rows.row_ref(i).expect("row");
			rendered.push((
				diff.kind(),
				r.f64("sum").expect("sum"),
				r.u64("start").expect("start"),
				r.u64("end").expect("end"),
			));
		}
	}
	rendered
}

macro_rules! harness {
	($driver:ty, $with:expr) => {
		ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<PlainDriver<$driver>>>::new().with($with).build()
	};
}

pub(crate) use harness;

fn sums_after(size: u64, pane: u64, feed: &[(u64, f64)]) -> Vec<f64> {
	let mut h = harness!(SumAnyKind, rolling(size, Some(pane), 3_600_000)).expect("harness");
	let mut sums = Vec::new();
	for (i, (ts, value)) in feed.iter().enumerate() {
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(i as u64 + 1, "BTC", *ts, *value)).build())
			.expect("apply");
		sums.extend(render(&out).into_iter().map(|(_, sum, _, _)| sum));
	}
	sums
}

#[test]
fn a_rolling_view_emits_the_merged_sum_of_its_panes_as_one_row_per_group() {
	// Each pane is a separate accumulator, so the emitted sum only grows if the engine merges them all.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 3_600_000)).expect("harness");
	let mut trace = Vec::new();
	for (i, (ts, value)) in [(0u64, 1.0), (1, 2.0), (2, 4.0)].into_iter().enumerate() {
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(i as u64 + 1, "BTC", ts, value)).build())
			.expect("apply");
		trace.extend(render(&out).into_iter().map(|(kind, sum, _, _)| (kind, sum)));
	}

	assert_eq!(trace, vec![(DiffType::Insert, 1.0), (DiffType::Update, 3.0), (DiffType::Update, 7.0)]);
}

#[test]
fn a_rolling_window_evicts_panes_beyond_the_size_over_the_pane() {
	// Capacity is size over pane; if it were ignored or off by one, the oldest pane would stay in the sum.
	let feed = [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0)];

	let three_panes = sums_after(3, 1, &feed);
	let two_panes = sums_after(2, 1, &feed);

	assert_eq!(three_panes, vec![1.0, 3.0, 7.0, 14.0], "3ms over 1ms panes keeps the newest three");
	assert_eq!(two_panes, vec![1.0, 3.0, 6.0, 12.0], "2ms over 1ms panes keeps the newest two");
}

#[test]
fn rows_inside_one_pane_share_it_and_do_not_count_toward_capacity() {
	// Bucketing is by the pane width; treating each row as its own pane would evict live rows early.
	let sums = sums_after(30, 10, &[(3, 1.0), (7, 2.0), (12, 4.0), (25, 8.0)]);

	assert_eq!(sums, vec![1.0, 3.0, 7.0, 15.0], "all four rows fall in three panes, within capacity 3");
}

#[test]
fn the_rolling_span_ends_at_the_newest_pane_and_reaches_back_one_size() {
	// The engine gives the guest a span; it must end where the newest pane ends, not at the row time.
	let mut h = harness!(SumAnyKind, rolling(30, Some(10), 3_600_000)).expect("harness");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1_003, 1.0)).build()).expect("apply");

	let start = DateTime::from_millis(980).to_order();
	let end = DateTime::from_millis(1_010).to_order();
	assert_eq!(render(&out), vec![(DiffType::Insert, 1.0, start, end)]);
}

#[test]
fn an_emptied_rolling_window_withdraws_its_row() {
	// A group whose panes all empty must retract its row; otherwise a stale total stays visible.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 5.0)).build()).expect("apply");

	let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 5.0)).build()).expect("apply");

	let kinds: Vec<DiffType> = render(&out).into_iter().map(|(kind, ..)| kind).collect();
	assert_eq!(kinds, vec![DiffType::Remove]);
}

#[test]
fn groups_roll_independently() {
	// The row key is the group alone; a shared key would merge two groups into one row.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 3_600_000)).expect("harness");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 1.0))
			.insert(input_row(2, "ETH", 0, 100.0))
			.build())
		.expect("apply");

	let mut sums: Vec<f64> = render(&out).into_iter().map(|(_, sum, _, _)| sum).collect();
	sums.sort_by(f64::total_cmp);
	assert_eq!(sums, vec![1.0, 100.0]);
}

#[test]
fn a_row_older_than_the_seal_horizon_is_dropped_and_a_recent_one_is_kept() {
	// Sealing is by pane time; if it never fired, late rows would silently reopen closed panes.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	let kept = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 95, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(110)).expect("watermark");

	let dropped = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 50, 4.0)).build()).expect("apply");

	assert!(!render(&kept).is_empty(), "a row inside the lateness window is still admitted");
	assert!(render(&dropped).is_empty(), "a row below the horizon must be dropped");
}

#[test]
fn the_same_operator_runs_tumbling_when_the_view_says_tumbling() {
	// One driver serves both kinds; a tumbling view must not be routed into the rolling engine.
	let mut h = harness!(SumAnyKind, tumbling(10)).expect("harness");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 13, 1.0)).build()).expect("apply");

	let start = DateTime::from_millis(10).to_order();
	let end = DateTime::from_millis(20).to_order();
	assert_eq!(render(&out), vec![(DiffType::Insert, 1.0, start, end)]);
}

#[test]
fn a_rolling_view_without_a_pane_reports_flow_075() {
	// Without a pane the engine cannot bucket rows or size the buffer; it must fail at create, not run blind.
	let err = harness!(SumAnyKind, rolling(3, None, 3_600_000)).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_075"), "expected FLOW_075, got: {err}");
}

#[test]
fn a_rolling_view_on_a_no_rolling_operator_reports_flow_066() {
	// A NoRolling operator has no valid pane merge; running it as rolling would emit wrong values.
	let err = harness!(SumTumblingOnly, rolling(3, Some(1), 3_600_000)).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}

#[test]
fn a_stopped_rolling_group_gives_back_its_meta_once_the_watermark_passes_it() {
	// The seal may add only its ledger row; a rolling driver that never expires leaves every dead group's meta.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");
	for i in 0..6u64 {
		h.apply(TestChangeBuilder::new().insert(input_row(i + 1, "BTC", 100 + i, 1.0)).build()).expect("apply");
	}
	let before = h.snapshot_state().len();

	h.advance_watermark(DateTime::from_millis(10_000_000)).expect("watermark");

	assert!(h.snapshot_state().len() <= before, "the seal must reclaim the dead group's meta, not add to state");
}

#[test]
fn a_withdrawn_rolling_group_leaves_only_its_meta_behind() {
	// Each withdrawn group may keep one row for the seal to reclaim; a leaked row number mapping doubles it.
	let state_after = |groups: u64| {
		let mut h = harness!(SumAnyKind, rolling(3, Some(1), 3_600_000)).expect("harness");
		for i in 0..groups {
			let group = format!("G{i}");
			h.apply(TestChangeBuilder::new().insert(input_row(i + 1, &group, 0, 5.0)).build())
				.expect("apply");
			h.apply(TestChangeBuilder::new().remove(input_row(i + 1, &group, 0, 5.0)).build())
				.expect("apply");
		}
		h.snapshot_state().len()
	};

	assert!(state_after(20) <= state_after(5) + 15, "state must grow by at most one row per withdrawn group");
}

#[test]
fn a_dead_rolling_group_arms_its_timer_one_size_past_the_seal() {
	// The timer must wait one size past the seal, otherwise it frees a group whose window still holds live panes.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");

	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 5.0)).build()).expect("apply");

	let dead: Vec<(DateTime, TimerKind)> =
		h.armed_timers().into_iter().filter(|t| t.key == b"rolling-dead").map(|t| (t.due, t.kind)).collect();
	assert_eq!(dead, vec![(DateTime::from_millis(117), TimerKind::Seal)]);
}

#[test]
fn a_dead_rolling_group_is_removed_on_its_timer_with_its_last_value() {
	// Without a Remove on the timer, a quiet group's last total stays visible forever.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 5.0)).build()).expect("apply");

	let out = h
		.on_timer(DateTime::from_millis(117), TimerKind::Seal, b"rolling-dead")
		.expect("timer")
		.expect("the dead group must emit");

	let start = DateTime::from_millis(98).to_order();
	let end = DateTime::from_millis(101).to_order();
	assert_eq!(render(&out), vec![(DiffType::Remove, 5.0, start, end)]);
}

#[test]
fn a_group_revived_inside_its_window_keeps_its_old_panes() {
	// A group must live one size past its horizon, otherwise a revived row loses the panes still in its window.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 1.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(115)).expect("watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 102, 2.0)).build()).expect("apply");

	let trace: Vec<(DiffType, f64)> = render(&out).into_iter().map(|(kind, sum, _, _)| (kind, sum)).collect();
	assert_eq!(trace, vec![(DiffType::Update, 3.0)]);
}

#[test]
fn a_quiet_stream_frees_every_dead_group() {
	// Each dead timer must re-arm for the next group, otherwise a quiet stream strands all but the first.
	let mut h = harness!(SumAnyKind, rolling(3, Some(1), 10)).expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "A", 100, 1.0))
		.insert(input_row(2, "B", 110, 2.0))
		.build())
		.expect("apply");

	h.advance_watermark(DateTime::from_millis(125)).expect("watermark");
	let dead: Vec<(DateTime, TimerKind)> =
		h.armed_timers().into_iter().filter(|t| t.key == b"rolling-dead").map(|t| (t.due, t.kind)).collect();
	assert_eq!(
		dead,
		vec![(DateTime::from_millis(127), TimerKind::Seal)],
		"the timer must re-arm on B after A dies"
	);

	assert_eq!(
		h.advance_watermark(DateTime::from_millis(130)).expect("watermark"),
		1,
		"only B's dead timer is due"
	);

	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "C", 200, 7.0)).build()).expect("apply");
	let trace: Vec<(DiffType, f64)> = render(&out).into_iter().map(|(kind, sum, _, _)| (kind, sum)).collect();
	assert_eq!(trace, vec![(DiffType::Remove, 1.0), (DiffType::Remove, 2.0), (DiffType::Insert, 7.0)]);
}
