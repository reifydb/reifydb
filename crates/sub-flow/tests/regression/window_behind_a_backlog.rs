// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
};
use reifydb_flow_async::window::{
	accumulator::invertible::moments::Moments, coord::DateTimeCoord, settings::WindowSettings, span::WindowSpan,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::{ColumnsView, RowView},
		windowed::operator::{AllKinds, Emit, PlainMarker, WindowedOperator},
	},
	row,
};
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, datetime::DateTime, duration::Duration, value_type::ValueType},
};

use crate::gate::{GatedPassThroughTimed, gate, set_gate};

const SETTLE: Duration = Duration::from_seconds_const(10);
const PRIMER_TS: &str = "2026-01-01T00:00:00.100Z";
const S1_TS: &str = "2026-01-01T00:00:00.200Z";

struct Timing {
	name: &'static str,
	lateness: &'static str,
	s2_ts: &'static str,
}

const TIMINGS: [Timing; 2] = [
	Timing {
		name: "far",
		lateness: "20s",
		s2_ts: "2026-01-01T00:00:30Z",
	},
	Timing {
		name: "near",
		lateness: "0s",
		s2_ts: "2026-01-01T00:00:01.500Z",
	},
];

#[derive(Debug, Clone, Copy)]
enum Window {
	NativeTumbling,
	NativeRolling,
	SdkTumbling,
	SdkRolling,
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct FoldWindow {
	k: String,
	start: DateTime,
	n: i64,
	total: i64,
}

row!(FoldWindow {
	k: String,
	start: DateTime,
	n: i64,
	total: i64
});

struct FoldWindowProbe;

impl WindowedOperator for FoldWindowProbe {
	type Coord = DateTimeCoord;
	type GroupKey = String;
	type Accumulator = Moments;
	type Output = FoldWindow;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> SdkResult<Vec<Option<DateTimeCoord>>> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> SdkResult<Option<DateTimeCoord>> {
						Ok(row.row_time().map(DateTimeCoord))
					})
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
	) -> SdkResult<Vec<Option<(String, f64)>>> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> SdkResult<Option<(String, f64)>> {
						let (Some(k), Some(v)) = (row.utf8("k")?, row.i64("v")?) else {
							return Ok(None);
						};
						Ok(Some((k.to_string(), v as f64)))
					})
					.transpose()
					.map(Option::flatten)
			})
			.collect()
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTimeCoord>) -> Moments {
		Moments::default()
	}
}

impl Emit for FoldWindowProbe {
	type Kinds = AllKinds;

	fn build_output(&self, group: &String, span: WindowSpan<DateTimeCoord>, value: &Moments) -> Option<FoldWindow> {
		Some(FoldWindow {
			k: group.clone(),
			start: span.start.0,
			n: i64::try_from(value.count()).expect("a window count fits in i64"),
			total: value.sum() as i64,
		})
	}
}

impl OperatorMetadata for FoldWindowProbe {
	const NAME: &'static str = "fold_window_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Counts and sums v per k over each window and reports the window start";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "k",
			type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
			description: "group key",
		},
		OperatorColumn {
			name: "v",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "counted and summed value",
		},
	];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "k",
			type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
			description: "group key",
		},
		OperatorColumn {
			name: "start",
			type_constraint: TypeConstraint::unconstrained(ValueType::DateTime),
			description: "window start",
		},
		OperatorColumn {
			name: "n",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "rows in the window",
		},
		OperatorColumn {
			name: "total",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "window sum",
		},
	];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn memory_db() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_flow(|f| {
				f.register_unmanaged_operator::<GatedPassThroughTimed>()
					.register_windowed_operator::<FoldWindowProbe, PlainMarker>()
			})
			.build()
			.expect("build memory db with flow"),
	)
}

fn settle(db: &TestDb) {
	assert!(db.await_all_flows(SETTLE), "flows never caught up");
}

fn window_body(window: Window, lateness: &str) -> String {
	let aggregates = "{ start: window::start(), n: math::count(), total: math::sum(v) }";
	match window {
		Window::NativeTumbling => {
			format!(
				"| window tumbling {aggregates} with {{ duration: 1s, lateness: {lateness} }} by {{ k }}"
			)
		}
		Window::NativeRolling => format!(
			"| window rolling {{ n: math::count(), total: math::sum(v) }} with {{ duration: 1s, pane: 100ms, lateness: {lateness} }} by {{ k }}"
		),
		Window::SdkTumbling => {
			format!(
				"APPLY fold_window_probe {{}} WITH {{ window: tumbling, duration: 1s, lateness: {lateness} }}"
			)
		}
		Window::SdkRolling => format!(
			"APPLY fold_window_probe {{}} WITH {{ window: rolling, duration: 1s, pane: 100ms, lateness: {lateness} }}"
		),
	}
}

fn create(db: &TestDb, gate_name: &str, window: Window, timing: &Timing) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8, ts: datetime } WITH { time: event(ts) }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::mid {{ k: utf8, v: int8, ts: datetime }} WITH {{ partition: {{ by: {{ k }} }} }} \
		 AS {{ FROM app::src APPLY gated_pass_through_timed {{ gate: '{gate_name}' }} }}"
	));
	let columns =
		window_columns(window).iter().map(|name| format!("{name}: {}", column_type(name))).collect::<Vec<_>>();
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::w {{ {} }} AS {{ FROM app::mid {} }}",
		columns.join(", "),
		window_body(window, timing.lateness)
	));
}

fn insert(db: &TestDb, k: &str, v: i64, ts: &str) {
	db.command(&format!(r#"INSERT app::src [{{ k: '{k}', v: {v}, ts: "{ts}" }}]"#));
}

fn delete(db: &TestDb, k: &str) {
	db.command(&format!("DELETE app::src FILTER {{ k == '{k}' }}"));
}

fn window_columns(window: Window) -> &'static [&'static str] {
	match window {
		Window::NativeRolling => &["k", "n", "total"],
		Window::NativeTumbling | Window::SdkTumbling | Window::SdkRolling => &["k", "start", "n", "total"],
	}
}

fn column_type(name: &str) -> &'static str {
	match name {
		"k" => "utf8",
		"start" => "datetime",
		_ => "int8",
	}
}

fn window_rows(db: &TestDb, window: Window) -> Vec<Vec<String>> {
	let mut rows = Vec::new();
	for frame in db.query("FROM app::w") {
		let columns: Vec<_> = window_columns(window)
			.iter()
			.map(|name| {
				frame.column(name).expect("column reads").expect("the window must expose its column")
			})
			.collect();
		for row in 0..columns[0].len() {
			rows.push(columns.iter().map(|column| format!("{:?}", column.get_value(row))).collect());
		}
	}
	rows.sort();
	rows
}

fn reference_run(window: Window, timing: &Timing) -> Vec<Vec<String>> {
	let db = memory_db();
	let gate_name = format!("{window:?}-{}-reference", timing.name);
	set_gate(&gate_name, true);
	create(&db, &gate_name, window, timing);
	settle(&db);
	insert(&db, "z", 1, PRIMER_TS);
	settle(&db);
	insert(&db, "a", 100, S1_TS);
	settle(&db);
	delete(&db, "a");
	insert(&db, "a", 120, timing.s2_ts);
	settle(&db);
	window_rows(&db, window)
}

fn backlog_run(window: Window, timing: &Timing) -> Vec<Vec<String>> {
	let db = memory_db();
	let gate_name = format!("{window:?}-{}-backlog", timing.name);
	set_gate(&gate_name, false);
	create(&db, &gate_name, window, timing);
	settle(&db);
	insert(&db, "z", 1, PRIMER_TS);
	assert!(
		poll_until(|| gate(&gate_name).entered.then_some(()), SETTLE).is_some(),
		"{}: the producer never started the step that holds the backlog",
		timing.name
	);
	insert(&db, "a", 100, S1_TS);
	delete(&db, "a");
	insert(&db, "a", 120, timing.s2_ts);
	set_gate(&gate_name, true);
	settle(&db);
	window_rows(&db, window)
}

fn assert_backlog_changes_nothing(window: Window) {
	for timing in &TIMINGS {
		let reference = reference_run(window, timing);
		assert!(
			reference.iter().any(|row| row[0].contains('a')),
			"{}: precondition: the reference run must publish a window for key a, got {reference:?}",
			timing.name
		);
		let backlog = backlog_run(window, timing);
		assert_eq!(
			backlog, reference,
			"{}: a backlog in the producer changed what the window published",
			timing.name
		);
	}
}

#[test]
fn a_native_tumbling_window_behind_a_backlogged_producer_matches_the_run_without_one() {
	// A producer that folds a=100@S1 and a=120@S2 into one commit can hide S1 from a bucket that sealed on it.
	assert_backlog_changes_nothing(Window::NativeTumbling);
}

#[test]
fn a_native_rolling_window_behind_a_backlogged_producer_matches_the_run_without_one() {
	// A folded backlog must not drop the pane S1 landed in before S2 moved the row past it.
	assert_backlog_changes_nothing(Window::NativeRolling);
}

#[test]
fn an_sdk_tumbling_window_behind_a_backlogged_producer_matches_the_run_without_one() {
	// The SDK window path must see S1 exactly as the native one does, otherwise its count and sum drift.
	assert_backlog_changes_nothing(Window::SdkTumbling);
}

#[test]
fn an_sdk_rolling_window_behind_a_backlogged_producer_matches_the_run_without_one() {
	// A folded backlog must not drop S1 from an SDK rolling window, otherwise its count and sum drift.
	assert_backlog_changes_nothing(Window::SdkRolling);
}
