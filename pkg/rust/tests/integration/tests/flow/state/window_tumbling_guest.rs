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
	window::{accumulator::invertible::Moments, span::WindowSpan},
};
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::Config,
	factory::time::secs,
	value::{constraint::TypeConstraint, datetime::DateTime, duration::Duration, value_type::ValueType},
};

const TIMEOUT: StdDuration = StdDuration::from_secs(15);

const APPLY_NODE_TYPE: u8 = 13;

const ACCUMULATORS: &str = "from system::metrics::flow::state::current
	filter { keyspace == 'ACCUMULATOR' }";

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
	type Accumulator = Moments;
	type Output = GuestWindow;

	fn extract(&self, _ctx: &mut impl GuestContext, row: &impl RowView) -> Option<(i32, f64)> {
		Some((row.i32("g")?, row.i32("v")? as f64))
	}

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime> {
		WindowSpan::for_coord(coord, secs(1))
	}

	fn seal_after(&self) -> Option<Duration> {
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
	// Group zero is the operator's own root scope, so only the other groups hold per-window state.
	format!("{} filter {{ group != 0 }}", state_of(operator))
}

fn state_of_group(operator: u64, group: u64) -> String {
	format!("{} filter {{ group == {group} }}", state_of(operator))
}

fn groups_holding_accumulators(db: &TestDb, operator: u64) -> Vec<u64> {
	// The group dimension is the only per-window identity the surface exposes, so it is how a survivor is named.
	let rql = format!("{} filter {{ keyspace == 'ACCUMULATOR' }}", state_of(operator));
	let frames = db.query(&rql);
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	column_values(frame, "group")
		.into_iter()
		.map(|value| match value {
			Value::Uint8(group) => group,
			other => panic!("the group dimension must be an unsigned id, found {other:?}"),
		})
		.collect()
}

fn state_shape(db: &TestDb, rql: &str) -> Vec<(Value, Value, Value)> {
	// Sample time moves every tick, so a comparable shape is what is stored, never when it was observed.
	let frames = db.query(rql);
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	column_values(frame, "keyspace")
		.into_iter()
		.zip(column_values(frame, "keys"))
		.zip(column_values(frame, "value_bytes"))
		.map(|((keyspace, keys), value_bytes)| (keyspace, keys, value_bytes))
		.collect()
}

fn advance_to(db: &TestDb, at: &str) {
	db.admin(&format!("call storage::advance(app::t, cast('{at}', datetime))"));
	db.await_all_flows(TIMEOUT);
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
	db.await_row_count(ACCUMULATORS, 2, TIMEOUT);
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

	let live = db.row_count(ACCUMULATORS);

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
		db.row_count(ACCUMULATORS),
		2,
		"a window inside its seal must keep its accumulator; surface now: {:?}",
		db.query(SURFACE)
	);
}

#[test]
fn a_sealed_guest_group_is_reaped_while_a_later_group_keeps_its_state_until_its_own_seal() {
	// A guest driver reaping by operator rather than by group would take the later group with it.
	let db = setup();
	guest_window(&db);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 5, ts: "2026-01-01T00:00:00.000Z" }]"#);
	db.await_row_count("FROM app::w", 1, TIMEOUT);
	db.await_row_count(ACCUMULATORS, 1, TIMEOUT);
	let operator = guest_operator(&db);
	let early = groups_holding_accumulators(&db, operator);
	assert_eq!(
		early.len(),
		1,
		"precondition: the first guest group must hold an accumulator; surface now: {:?}",
		db.query(SURFACE)
	);

	// This insert carries the frontier past the first window's seal, which is what reaps it.
	db.command(r#"INSERT app::t [{ id: 2, g: 2, v: 7, ts: "2026-01-01T00:00:10.000Z" }]"#);
	db.await_row_count("FROM app::w", 2, TIMEOUT);
	db.await_all_flows(TIMEOUT);

	let sealed = state_of_group(operator, early[0]);
	assert_eq!(
		db.await_exact_row_count(&sealed, 0, TIMEOUT),
		0,
		"the sealed guest group must leave nothing addressable behind; surface now: {:?}",
		db.query(&state_of(operator))
	);

	let late = groups_holding_accumulators(&db, operator);
	assert_eq!(
		late.len(),
		1,
		"the first group sealed and the second is still open, so exactly one accumulator may remain; surface now: {:?}",
		db.query(SURFACE)
	);
	assert!(
		!late.contains(&early[0]),
		"the survivor must be the later group, not the one that sealed; surface now: {:?}",
		db.query(SURFACE)
	);

	let untouched = state_shape(&db, &state_of_group(operator, late[0]));
	assert!(
		!untouched.is_empty(),
		"precondition: the open guest group must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_to(&db, "2026-01-01T00:00:11.000Z");

	assert_eq!(
		state_shape(&db, &state_of_group(operator, late[0])),
		untouched,
		"a reap pass not yet due for this group must not touch a byte of it; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_to(&db, "2026-01-01T00:01:00.000Z");

	assert_eq!(
		db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT),
		0,
		"the later guest group must be reaped once its own seal falls due; surface now: {:?}",
		db.query(SURFACE)
	);
	assert_eq!(
		db.await_exact_row_count(&per_window_state_of(operator), 0, TIMEOUT),
		0,
		"neither guest group may leave per-window state behind; surface now: {:?}",
		db.query(&state_of(operator))
	);

	assert_eq!(db.row_count("FROM app::w FILTER { total == 5 }"), 1, "neither reap may retract a published row");
	assert_eq!(db.row_count("FROM app::w FILTER { total == 7 }"), 1);
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
		db.row_count(&per_window) > 0,
		"precondition: open guest windows must report state; surface now: {:?}",
		db.query(SURFACE)
	);

	advance_past_the_reap(&db);

	let reaped = db.await_exact_row_count(ACCUMULATORS, 0, TIMEOUT);
	assert_eq!(
		reaped,
		0,
		"a sealed guest window's accumulator must not outlive the reap; surface now: {:?}",
		db.query(SURFACE)
	);

	let remaining = db.await_exact_row_count(&per_window, 0, TIMEOUT);
	assert_eq!(
		remaining,
		0,
		"a sealed guest window must keep neither its accumulator nor the mapping addressing it; surface now: {:?}",
		db.query(&state_of(operator))
	);
}
