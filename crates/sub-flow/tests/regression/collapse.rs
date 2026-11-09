// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Value, WithSubsystem, embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::{
	common::{CommitVersion, SourceVersion},
	interface::catalog::{id::ViewId, object::ObjectId},
};
use reifydb_engine::lift::entry_changed_objects;
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_value::value::duration::Duration;

use crate::{
	gate::{GatedPassThrough, gate, set_gate},
	layers::layer_sources,
};

const SETTLE: Duration = Duration::from_seconds_const(10);

const BACKLOG: &[&str] = &[
	"INSERT app::src [{ k: 'a', v: 0 }]",
	r#"INSERT app::level [{ id: 10, k: 'a', at: "2026-01-01T00:00:01Z" }]"#,
	"UPDATE app::src { v: 1 } FILTER { k == 'a' }",
	"INSERT app::ring [{ k: 'a', v: 1 }]",
	r#"INSERT app::level [{ id: 11, k: 'a', at: "2026-01-01T00:00:02Z" }]"#,
	"UPDATE app::src { v: 2 } FILTER { k == 'a' }",
	"INSERT app::src [{ k: 'b', v: 5 }]",
	"DELETE app::src FILTER { k == 'b' }",
	"DELETE app::src FILTER { k == 'z' }",
	"INSERT app::src [{ k: 'z', v: 9 }]",
	"INSERT app::ring [{ k: 'z', v: 9 }]",
];

fn memory_db() -> TestDb {
	let mut db = TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_flow(|f| f.register_unmanaged_operator::<GatedPassThrough>())
			.build()
			.expect("build memory db with flow"),
	);
	db.hold_commits();
	db
}

fn settle(db: &TestDb) {
	assert!(db.await_all_flows(SETTLE), "flows never caught up");
}

fn create_base(db: &TestDb, gate_name: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::src2 { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::level { id: int4, k: utf8, at: datetime } WITH { time: event(at) }");
	db.admin("CREATE RINGBUFFER app::ring { k: utf8, v: int8 } WITH { capacity: 16 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::price {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ \
		 FROM app::src APPLY gated_pass_through {{ gate: '{gate_name}' }} }}"
	));
	db.command("INSERT app::src2 [{ k: 'c', v: 7 }]");
	for (id, k) in [(1, "a"), (2, "b"), (3, "z"), (4, "c")] {
		db.command(&format!(r#"INSERT app::level [{{ id: {id}, k: '{k}', at: "2026-01-01T00:00:00Z" }}]"#));
	}
}

fn current(db: &TestDb) -> CommitVersion {
	db.engine().current_version().expect("the current version after a write")
}

fn run_script(db: &TestDb, gate_name: &str, held: bool, extra: Option<&str>) -> Vec<CommitVersion> {
	set_gate(gate_name, !held);
	db.command("INSERT app::src [{ k: 'z', v: 0 }]");
	if held {
		assert!(
			poll_until(|| gate(gate_name).entered.then_some(()), SETTLE).is_some(),
			"the producer never started the step that holds the backlog"
		);
	} else {
		settle(db);
	}
	let mut written = Vec::new();
	for (at, statement) in BACKLOG.iter().enumerate() {
		db.command(statement);
		written.push(current(db));
		if at == BACKLOG.len() / 2
			&& let Some(admin) = extra
		{
			db.admin(admin);
			written.push(current(db));
		}
	}
	set_gate(gate_name, true);
	settle(db);
	written
}

fn view_object(db: &TestDb, name: &str) -> ObjectId {
	let frames = db.query(&format!("FROM system::views FILTER {{ name == '{name}' }}"));
	let frame = frames.first().expect("system catalog frame");
	let ids = frame.column("id").expect("id reads").expect("system::views must expose an id column");
	assert_eq!(ids.len(), 1, "'{name}' must name exactly one view");
	match ids.get_value(0) {
		Value::Uint8(id) => ObjectId::View(ViewId(id)),
		other => panic!("'{name}' has no numeric id in system::views, got {other:?}"),
	}
}

fn rows(db: &TestDb, view: &str, columns: &[&str]) -> Vec<String> {
	let frames = db.query(&format!("FROM app::{view}"));
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	let read: Vec<_> = columns
		.iter()
		.map(|name| frame.column(name).expect("column reads").unwrap_or_else(|| panic!("{view} has no {name}")))
		.collect();
	let count = read.first().map_or(0, |column| column.len());
	let mut out: Vec<String> = (0..count)
		.map(|row| {
			read.iter().map(|column| format!("{:?}", column.get_value(row))).collect::<Vec<_>>().join("|")
		})
		.collect();
	out.sort();
	out
}

fn layers_after(db: &TestDb, view: &str, from: CommitVersion) -> Vec<SourceVersion> {
	layer_sources(db, view_object(db, view))
		.into_iter()
		.filter(|source| *source >= SourceVersion::from(from))
		.collect()
}

fn largest_commit_layers(db: &TestDb, view: &str, from: CommitVersion) -> usize {
	let object = view_object(db, view);
	db.commit_history()
		.iter()
		.filter(|entry| entry_changed_objects(entry).contains(&object))
		.filter(|entry| entry.version.source >= SourceVersion::from(from))
		.map(|entry| entry.layers.len().max(1))
		.max()
		.unwrap_or(0)
}

struct Case<'a> {
	name: &'a str,
	views: &'a [&'a str],
	checked: &'a str,
	columns: &'a [&'a str],
	extra: Option<&'a str>,
}

fn run_case(case: &Case, held: bool) -> (TestDb, Vec<CommitVersion>) {
	let db = memory_db();
	let gate_name = format!(
		"collapse_{}_{}",
		case.name,
		if held {
			"held"
		} else {
			"reference"
		}
	);
	set_gate(&gate_name, true);
	create_base(&db, &gate_name);
	for view in case.views {
		db.admin(view);
	}
	settle(&db);
	let written = run_script(&db, &gate_name, held, case.extra);
	(db, written)
}

fn assert_matches_reference(case: &Case) -> (TestDb, Vec<CommitVersion>) {
	let (held, written) = run_case(case, true);
	let (reference, _) = run_case(case, false);
	let expected = rows(&reference, case.checked, case.columns);
	assert!(!expected.is_empty(), "precondition: the reference {} must hold rows", case.checked);
	let converged = poll_until(
		|| {
			(rows(&held, case.checked, case.columns) == rows(&reference, case.checked, case.columns))
				.then_some(())
		},
		SETTLE,
	);
	assert!(
		converged.is_some(),
		"{} after a backlog must equal the no-backlog reference: held {:?}, reference {:?}",
		case.checked,
		rows(&held, case.checked, case.columns),
		rows(&reference, case.checked, case.columns)
	);
	(held, written)
}

fn assert_one_layer_per_commit(held: &TestDb, view: &str, written: &[CommitVersion]) {
	assert!(
		largest_commit_layers(held, "price", written[0]) > 1,
		"precondition: the held producer must hand the backlog on as several sources in one commit"
	);
	assert_eq!(
		largest_commit_layers(held, view, written[0]),
		1,
		"{view} must collapse every step it takes from the backlog into one layer"
	);
}

fn assert_collapsed(case: &Case) {
	let (held, written) = assert_matches_reference(case);
	assert_one_layer_per_commit(&held, case.checked, &written);
}

fn reader(schema: &str, body: &str) -> String {
	format!("CREATE DEFERRED VIEW app::out {{ {schema} }} AS {{ {body} }}")
}

fn one_view_case(name: &str, schema: &str, body: &str, columns: &[&str]) {
	let view = reader(schema, body);
	assert_collapsed(&Case {
		name,
		views: &[&view],
		checked: "out",
		columns,
		extra: None,
	});
}

#[test]
fn a_table_source_reader_collapses_its_backlog_to_the_reference_state() {
	// A table read straight from storage must fold its pending writes to their net change and still end exact.
	one_view_case(
		"source_table",
		"k: utf8, v: int8, pv: Option(int8)",
		"FROM app::src LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) MAP { k, v, pv: p_v }",
		&["k", "v", "pv"],
	);
}

#[test]
fn a_view_source_reader_collapses_its_backlog_to_the_reference_state() {
	// A chain of updates, an insert then remove, and a remove then insert must leave only their net effect.
	one_view_case("source_view", "k: utf8, v: int8", "FROM app::price", &["k", "v"]);
}

#[test]
fn a_ring_buffer_source_reader_collapses_its_backlog_to_the_reference_state() {
	// Ring buffer rows arrive as appends, and folding them must keep every appended row.
	one_view_case(
		"source_ring",
		"k: utf8, v: int8, pv: Option(int8)",
		"FROM app::ring LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) MAP { k, v, pv: p_v }",
		&["k", "v", "pv"],
	);
}

#[test]
fn a_filter_reader_collapses_its_backlog_to_the_reference_state() {
	// A row filtered out at one source and in at a later one must end in the view exactly as per source.
	one_view_case("filter", "k: utf8, v: int8", "FROM app::price FILTER { v != 1 }", &["k", "v"]);
}

#[test]
fn a_map_reader_collapses_its_backlog_to_the_reference_state() {
	// A mapped row must be computed from the row's last value, never an earlier one in the fold.
	one_view_case("map", "k: utf8, w: int8", "FROM app::price MAP { k, w: v * 2 }", &["k", "w"]);
}

#[test]
fn an_extend_reader_collapses_its_backlog_to_the_reference_state() {
	// An extended column must follow the row's last value inside the fold.
	one_view_case("extend", "k: utf8, v: int8, w: int8", "FROM app::price EXTEND { w: v + 1 }", &["k", "v", "w"]);
}

#[test]
fn a_hash_join_reader_collapses_its_backlog_to_the_reference_state() {
	// A hash join keeps both sides in state, so a folded right side must still pair every left row once.
	one_view_case(
		"hash_join",
		"id: int4, pv: int8",
		"FROM app::level INNER JOIN { FROM app::price } AS p USING (k, p.k) MAP { id, pv: p_v }",
		&["id", "pv"],
	);
}

#[test]
fn a_latest_join_reader_collapses_its_backlog_to_the_reference_state() {
	// The latest right row after the fold must be the one each left row ends paired with.
	one_view_case(
		"latest_join",
		"id: int4, pv: int8",
		"FROM app::level INNER JOIN { FROM app::price } AS p USING (k, p.k) WITH { latest: true } MAP { id, pv: p_v }",
		&["id", "pv"],
	);
}

#[test]
fn a_latest_left_join_reader_collapses_its_backlog_to_the_reference_state() {
	// A fold must leave every left row, paired or not, with exactly the row id and value of the no-backlog run.
	one_view_case(
		"latest_left_join",
		"id: int4, pv: Option(int8)",
		"FROM app::level LEFT JOIN { FROM app::price } AS p USING (k, p.k) WITH { latest: true } MAP { id, pv: p_v }",
		&["#rownum", "id", "pv"],
	);
}

#[test]
fn an_earliest_join_reader_collapses_its_backlog_to_the_reference_state() {
	// The earliest pick runs over the same right state, so a fold must not change its winner.
	one_view_case(
		"earliest_join",
		"id: int4, pv: int8",
		"FROM app::level INNER JOIN { FROM app::price } AS p USING (k, p.k) WITH { earliest: true } MAP { id, pv: p_v }",
		&["id", "pv"],
	);
}

#[test]
fn a_column_pick_join_reader_collapses_its_backlog_to_the_reference_state() {
	// A pick by a column must choose its winner from the final right rows, the same as per source.
	one_view_case(
		"column_pick_join",
		"id: int4, pv: int8",
		"FROM app::level INNER JOIN { FROM app::price } AS p USING (k, p.k) WITH { latest: { p.v } } MAP { id, pv: p_v }",
		&["id", "pv"],
	);
}

#[test]
fn an_emit_on_lookup_reader_collapses_its_backlog_to_the_reference_state() {
	// A re-emitting lookup must end on the right side's final value for every left row.
	one_view_case(
		"lookup_on",
		"id: int4, pv: Option(int8)",
		"FROM app::level LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) MAP { id, pv: p_v }",
		&["id", "pv"],
	);
}

#[test]
fn an_emit_after_lookup_reader_collapses_its_backlog_to_the_reference_state() {
	// A paced lookup's timer flush is its own commit, so collapse shows as no commit holding more than one layer.
	let view = reader(
		"id: int4, pv: Option(int8)",
		"FROM app::level LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: 1s } MAP { id, pv: p_v }",
	);
	let (held, written) = assert_matches_reference(&Case {
		name: "lookup_after",
		views: &[&view],
		checked: "out",
		columns: &["id", "pv"],
		extra: None,
	});
	assert_one_layer_per_commit(&held, "out", &written);
}

#[test]
fn a_producer_read_by_an_emit_on_lookup_in_an_aggregate_collapses_and_the_aggregate_stays_exact() {
	// A non-collapsing reader re-reads on each right change, so its producer's net change must keep its totals.
	let mid = "CREATE DEFERRED VIEW app::mid { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { FROM app::price }".to_string();
	let agg = "CREATE DEFERRED VIEW app::agg { k: utf8, total: int8 } AS { FROM app::level \
	           INNER LOOKUP { FROM app::mid } AS m USING (k, m.k) AGGREGATE { total: math::sum(m_v) } BY { k } }"
		.to_string();
	let case = Case {
		name: "aggregate_reader",
		views: &[&mid, &agg],
		checked: "agg",
		columns: &["k", "total"],
		extra: None,
	};
	let (held, written) = assert_matches_reference(&case);
	assert_one_layer_per_commit(&held, "mid", &written);
}

#[test]
fn a_live_created_emit_off_reader_keeps_its_producer_one_layer_per_source() {
	// A producer that stays collapsed after an exact reader appears pairs the reader's rows with a later price.
	let mid = "CREATE DEFERRED VIEW app::mid { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { FROM app::price }".to_string();
	let cost = "CREATE DEFERRED VIEW app::cost { id: int4, usd: Option(int8) } AS { FROM app::level \
	            LEFT LOOKUP { FROM app::mid } AS p USING (k, p.k) WITH { emit: false, retention: { left: 10s } } \
	            MAP { id, usd: p_v } }"
		.to_string();
	let case = Case {
		name: "exact_reader",
		views: &[&mid, &cost],
		checked: "cost",
		columns: &["id", "usd"],
		extra: None,
	};
	let (held, written) = assert_matches_reference(&case);
	let layers = layers_after(&held, "mid", written[0]);
	assert!(layers.len() > 1, "an exact reader must keep mid at one layer per source, got {layers:?}");
}

#[test]
fn a_collapsed_backlog_holding_a_completeness_assertion_still_ends_exact() {
	// A completeness change inside a collapsed step must not crash, stall or split the step; its watermark effect
	// is unobservable here.
	let view = reader(
		"id: int4, pv: int8",
		"FROM app::level INNER JOIN { FROM app::price } AS p USING (k, p.k) WITH { latest: true } MAP { id, pv: p_v }",
	);
	assert_collapsed(&Case {
		name: "completeness",
		views: &[&view],
		checked: "out",
		columns: &["id", "pv"],
		extra: Some("call storage::advance(app::level, cast('2026-01-01T00:00:30Z', datetime))"),
	});
}
