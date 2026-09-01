// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{
	ConfigKey, Value, WithSubsystem,
	codec::key::encoded::EncodedKey,
	core::interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	embedded,
	sdk::{
		error::Result as SdkResult,
		flow::operator::{
			column::operator::OperatorColumn,
			context::GuestContext,
			view::RowView,
			windowed::tumbling::{TumblingDriver, TumblingOperator, TumblingRegistration},
		},
		row,
	},
	seal::coord::Coord,
	testing::db::TestDb,
	window::{accumulator::invertible::moments::Moments, span::WindowSpan},
};
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::Config,
	factory::time::secs,
	value::{constraint::TypeConstraint, datetime::DateTime, duration::Duration, value_type::ValueType},
};

use crate::flow::state::{await_state_keys, state_keys};

const TIMEOUT: StdDuration = StdDuration::from_secs(15);

const APPLY_NODE_TYPE: u8 = 13;

const ACCUMULATORS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'GUEST_ACCUMULATOR' }";

const SURFACE: &str = "from system::metrics::flow::state::current";

#[derive(Clone, Debug, PartialEq)]
struct GuestWindow {
	g: i32,
	total: i64,
}

row!(GuestWindow {
	g: i32,
	total: i64
});

struct GuestTumbling;

impl TumblingOperator for GuestTumbling {
	type GroupKey = i32;

	type WindowSlot = DateTime;

	type Accumulator = Moments;
	type Output = GuestWindow;

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext, row: &impl RowView) -> Option<(i32, f64)> {
		Some((row.i32("g")?, row.i32("v")? as f64))
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, secs(1))
	}

	fn lateness(&self) -> Option<Duration> {
		Some(secs(1))
	}

	fn build_output(&self, group: &i32, _span: WindowSpan<DateTime>, value: Moments) -> Option<GuestWindow> {
		Some(GuestWindow {
			g: *group,
			total: value.sum() as i64,
		})
	}
}

impl TumblingRegistration for GuestTumbling {
	const NAME: &'static str = "tumbling_guest";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Sums v per g over one-second tumbling windows";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "g",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Group key",
		},
		OperatorColumn {
			name: "v",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Summed value",
		},
	];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[
		OperatorColumn {
			name: "g",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
			description: "Group key",
		},
		OperatorColumn {
			name: "total",
			type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
			description: "Window sum",
		},
	];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Self)
	}

	fn encode_row_key(&self, group: &i32, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().i32(*group).u64(window_start.to_order()).build()
	}
}

fn setup() -> TestDb {
	// The guest operator is registered in process, so no dylib is built and the ABI plays no part here.
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<TumblingDriver<GuestTumbling>>())
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with a registered guest operator"),
	)
}

fn guest_operator(db: &TestDb) -> u64 {
	let rql = format!("FROM system::operators FILTER {{ node_type == {APPLY_NODE_TYPE} }} MAP {{ id }}");
	let frames = db.query(&rql);
	let values = column_values(frames.first().expect("system::operators returned no frame"), "id");
	match values.as_slice() {
		[Value::Uint8(id)] => *id,
		other => panic!("expected exactly one applied guest operator, found {other:?}"),
	}
}

fn state_of(operator: u64) -> String {
	format!("from system::metrics::flow::state::current filter {{ operator == {operator} }}")
}

fn per_window_state_of(operator: u64) -> String {
	// The counters, the ledger and the meta are the operator's own bookkeeping, so the rest is per window.
	format!(
		"{} filter {{ keyspace != 'NODE_COUNTER' and keyspace != 'SEAL_LEDGER' and keyspace != 'WINDOW_META' }}",
		state_of(operator)
	)
}

fn guest_window(db: &TestDb) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::w { g: int4, total: int8 } AS {
				FROM app::t
					| apply tumbling_guest{}
			}"#);
}

fn fill_first_window(db: &TestDb) {
	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:00:00.500Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);
	await_state_keys(db, ACCUMULATORS, 2, TIMEOUT);
}

fn advance_past_the_reap(db: &TestDb) {
	db.admin("call storage::advance(app::t, cast('2026-01-01T00:01:00Z', datetime))");
	db.await_all_flows(TIMEOUT);
}

#[test]
fn a_live_guest_window_reports_the_accumulator_holding_its_aggregate() {
	// Without a surface that sees live guest state, no later assertion about reaping can mean anything.
	let db = setup();
	guest_window(&db);
	fill_first_window(&db);

	let live = state_keys(&db, ACCUMULATORS);

	assert_eq!(
		live,
		2,
		"two open guest windows must each report an accumulator; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_guest_window_that_is_still_open_keeps_its_accumulator_through_a_reap() {
	// Reaping an unsealed window would drop an aggregate that is still accepting contributions.
	let db = setup();
	guest_window(&db);
	fill_first_window(&db);

	db.admin("call storage::advance(app::t, cast('2026-01-01T00:00:00.900Z', datetime))");
	db.await_all_flows(TIMEOUT);

	assert_eq!(
		state_keys(&db, ACCUMULATORS),
		2,
		"a window inside its seal must keep its accumulator; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn the_sealed_guest_windows_published_rows_survive_the_reap() {
	// Reclamation must be silent, so freeing state must never retract what the view published.
	let db = setup();
	guest_window(&db);
	fill_first_window(&db);

	advance_past_the_reap(&db);

	assert_eq!(
		db.row_count("FROM app::w FILTER { total == 5 }"),
		1,
		"sealing a guest window must not remove the row it published"
	);
	assert_eq!(db.row_count("FROM app::w FILTER { total == 7 }"), 1);
}

#[test]
fn a_sealed_guest_window_has_both_its_data_and_its_identity_reaped() {
	// A guest operator that seals without reaping keeps every window it ever opened, growing without bound.
	let db = setup();
	guest_window(&db);
	fill_first_window(&db);
	let operator = guest_operator(&db);
	let per_window = per_window_state_of(operator);
	assert!(
		state_keys(&db, &per_window) > 0,
		"precondition: open guest windows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_reap(&db);

	let reaped = await_state_keys(&db, ACCUMULATORS, 0, TIMEOUT);
	assert_eq!(
		reaped,
		0,
		"a sealed guest window's accumulator must not outlive the reap; surface now: {:?}",
		db.query(SURFACE)
	);

	let remaining = await_state_keys(&db, &per_window, 0, TIMEOUT);
	assert_eq!(
		remaining,
		0,
		"a sealed guest window must keep neither its accumulator nor the mapping addressing it; surface now: {:?}",
		db.query(&state_of(operator))
	);
}
