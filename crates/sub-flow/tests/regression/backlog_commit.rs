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

use crate::gate::{GatedPassThrough, gate, set_gate};

const SETTLE: Duration = Duration::from_seconds_const(10);
const ROUNDS: usize = 3;
const T0: &str = "2026-01-01T00:00:00Z";

fn disarmed() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory_db() -> TestDb {
	let mut db = TestDb::from(
		embedded::memory()
			.with_runtime_config(disarmed())
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

fn create_price(db: &TestDb, from: &str, gate: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::src2 { k: utf8, v: int8 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::price {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ \
		 FROM {from} APPLY gated_pass_through {{ gate: '{gate}' }} }}"
	));
}

fn create_level(db: &TestDb) {
	db.admin("CREATE TABLE app::level { id: int4, k: utf8, at: datetime } WITH { time: event(at) }");
}

fn insert_level(db: &TestDb, id: i32, k: &str) {
	db.command(&format!(r#"INSERT app::level [{{ id: {id}, k: '{k}', at: "{T0}" }}]"#));
}

fn write_src(db: &TestDb, k: &str, v: i64, insert: bool) -> CommitVersion {
	if insert {
		db.command(&format!("INSERT app::src [{{ k: '{k}', v: {v} }}]"));
	} else {
		db.command(&format!("UPDATE app::src {{ v: {v} }} FILTER {{ k == '{k}' }}"));
	}
	db.engine().current_version().expect("the current version after a source write")
}

fn hold(db: &TestDb, gate_name: &str) {
	set_gate(gate_name, false);
	write_src(db, "z", 0, true);
	assert!(
		poll_until(|| gate(gate_name).entered.then_some(()), SETTLE).is_some(),
		"the producer never started the step that holds the backlog"
	);
}

fn backlog(db: &TestDb, gate_name: &str, rounds: usize) -> Vec<CommitVersion> {
	hold(db, gate_name);
	let written: Vec<CommitVersion> =
		(0..rounds).map(|round| write_src(db, "a", round as i64, round == 0)).collect();
	set_gate(gate_name, true);
	settle(db);
	written
}

fn price_view(db: &TestDb) -> ObjectId {
	let frames = db.query("FROM system::views FILTER { name == 'price' }");
	let frame = frames.first().expect("system catalog frame");
	let ids = frame.column("id").expect("id reads").expect("system::views must expose an id column");
	assert_eq!(ids.len(), 1, "'price' must name exactly one view");
	match ids.get_value(0) {
		Value::Uint8(id) => ObjectId::View(ViewId(id)),
		other => panic!("'price' has no numeric id in system::views, got {other:?}"),
	}
}

fn price_commits_after(db: &TestDb, from: CommitVersion) -> usize {
	let price = price_view(db);
	db.commit_history()
		.iter()
		.filter(|entry| entry_changed_objects(entry).contains(&price))
		.filter(|entry| entry.version.source >= SourceVersion::from(from))
		.count()
}

fn assert_backlog_formed(written: &[CommitVersion]) {
	assert_eq!(written.len(), ROUNDS, "precondition: the backlog must hold every round");
	assert!(
		written.windows(2).all(|pair| pair[0] < pair[1]),
		"precondition: each round must be its own source commit, got {written:?}"
	);
}

#[test]
fn a_view_read_only_by_from_folds_its_backlog_into_one_commit() {
	// A plain reader reads the folded state fine, so it must not hold its producer to one commit per version.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_e1");
	db.admin("CREATE DEFERRED VIEW app::r1 { k: utf8, v: int8 } AS { FROM app::price }");
	db.admin("CREATE DEFERRED VIEW app::r2 { k: utf8, v: int8 } AS { FROM app::price }");
	settle(&db);

	let written = backlog(&db, "backlog_commit_e1", ROUNDS);
	assert_backlog_formed(&written);
	assert_eq!(db.row_count("FROM app::r1"), 2, "precondition: the first reader saw both keys");
	assert_eq!(price_commits_after(&db, written[0]), 1, "a producer read only by FROM must fold its backlog");
}

#[test]
fn a_looked_up_view_read_with_emit_after_folds_its_backlog() {
	// A lookup that emits on a timer never reads one source version, so it must not force per-version commits.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_e2");
	create_level(&db);
	db.admin("CREATE DEFERRED VIEW app::cost { id: int4, usd: Option(int8) } AS { \
		 FROM app::level \
		 INNER LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: 1s, retention: { left: 10s } } \
		 MAP { id, usd: p_v } }");
	settle(&db);

	let written = backlog(&db, "backlog_commit_e2", ROUNDS);
	assert_backlog_formed(&written);
	assert_eq!(price_commits_after(&db, written[0]), 1, "a producer read by an emit-after lookup must fold");
}

#[test]
fn a_view_on_the_right_of_a_latest_emit_on_join_folds() {
	// An emit-on join re-reads the newest right row on each change, so it must not force per-version commits.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_e3");
	create_level(&db);
	db.admin("CREATE DEFERRED VIEW app::cost { id: int4, usd: int8 } AS { \
		 FROM app::level \
		 INNER JOIN { FROM app::price } AS p USING (k, p.k) WITH { emit: true, latest: true } \
		 MAP { id, usd: p_v } }");
	settle(&db);

	let written = backlog(&db, "backlog_commit_e3", ROUNDS);
	assert_backlog_formed(&written);
	assert_eq!(price_commits_after(&db, written[0]), 1, "a producer on the right of an emit-on join must fold");
}

#[test]
fn an_append_of_two_tables_folds_its_backlog() {
	// An append pairs nothing by version, so counting its two sources caps it at one commit per version.
	let db = memory_db();
	create_price(&db, "app::src append { FROM app::src2 }", "backlog_commit_e4");
	settle(&db);

	let written = backlog(&db, "backlog_commit_e4", ROUNDS);
	assert_backlog_formed(&written);
	assert_eq!(db.row_count("FROM app::price"), 2, "precondition: the append saw both keys");
	assert_eq!(price_commits_after(&db, written[0]), 1, "an append of two tables with no exact reader must fold");
}

#[test]
fn a_producer_starts_committing_per_version_once_an_exact_reader_is_created() {
	// A producer left folding after an exact reader appears pairs that reader's rows with a later price.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_e5");
	create_level(&db);
	settle(&db);
	db.admin("CREATE DEFERRED VIEW app::cost { id: int4, usd: Option(int8) } AS { \
		 FROM app::level \
		 INNER LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: false, retention: { left: 10s } } \
		 MAP { id, usd: p_v } }");
	settle(&db);

	hold(&db, "backlog_commit_e5");
	let first = write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	let second = write_src(&db, "a", 20, false);
	insert_level(&db, 2, "a");
	set_gate("backlog_commit_e5", true);
	settle(&db);

	assert!(first < second, "precondition: each price is its own source commit");
	assert_eq!(price_commits_after(&db, first), 2, "a producer with a new exact reader must commit each price");
	let frames = db.query("FROM app::cost SORT { id: asc }");
	let frame = frames.first().expect("cost frame");
	let ids = frame.column("id").expect("id reads").expect("cost has id");
	let usd = frame.column("usd").expect("usd reads").expect("cost has usd");
	let rows: Vec<(Value, Value)> = (0..ids.len()).map(|row| (ids.get_value(row), usd.get_value(row))).collect();
	assert_eq!(
		rows,
		vec![(Value::Int4(1), Value::Int8(10)), (Value::Int4(2), Value::Int8(20))],
		"each left row must pair with the price committed at or below its own source"
	);
}
