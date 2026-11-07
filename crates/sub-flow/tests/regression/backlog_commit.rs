// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Value, WalMode, WithSubsystem, embedded,
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

fn memory_db_without_commit_log() -> TestDb {
	let mut db = TestDb::from(
		embedded::memory()
			.with_runtime_config(disarmed())
			.with_wal(|wal| wal.mode(WalMode::Off))
			.with_flow(|f| f.register_unmanaged_operator::<GatedPassThrough>())
			.build()
			.expect("build memory db with flow and no commit log"),
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
fn a_producer_stays_exact_after_an_exact_reader_is_created() {
	// A fold read past its layers after an exact reader appears pairs that reader's rows with a later price.
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
	assert!(price_commits_after(&db, first) < 2, "a producer with a new exact reader must still fold its backlog");
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

fn create_cost(db: &TestDb, view: &str) {
	create_level(db);
	db.admin(&format!("CREATE DEFERRED VIEW app::cost {{ id: int4, usd: Option(int8) }} AS {{ \
		 FROM app::level \
		 LEFT LOOKUP {{ FROM {view} }} AS p USING (k, p.k) WITH {{ emit: false, retention: {{ left: 10s }} }} \
		 MAP {{ id, usd: p_v }} }}"));
}

fn cost_rows(db: &TestDb) -> Vec<(Value, Option<Value>)> {
	let frames = db.query("FROM app::cost SORT { id: asc }");
	let frame = frames.first().expect("cost frame");
	let ids = frame.column("id").expect("id reads").expect("cost has id");
	let usd = frame.column("usd").expect("usd reads").expect("cost has usd");
	(0..ids.len())
		.map(|row| match usd.get_value(row) {
			Value::None {
				..
			} => (ids.get_value(row), None),
			value => (ids.get_value(row), Some(value)),
		})
		.collect()
}

fn write_table(db: &TestDb, table: &str, k: &str, v: i64, insert: bool) {
	if insert {
		db.command(&format!("INSERT {table} [{{ k: '{k}', v: {v} }}]"));
	} else {
		db.command(&format!("UPDATE {table} {{ v: {v} }} FILTER {{ k == '{k}' }}"));
	}
}

#[test]
fn an_aggregate_behind_a_backlog_is_read_at_each_source() {
	// A later source in one step must build on the state an earlier source wrote and keep its own layer;
	// otherwise the reader at the first source sees the final total, or the second total misses the first row.
	let db = memory_db();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE DEFERRED VIEW app::price { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { \
		 FROM app::src APPLY gated_pass_through { gate: 'backlog_commit_b1_3' } \
		 AGGREGATE { v: math::sum(v) } BY { k } }");
	create_cost(&db, "app::price");
	settle(&db);

	hold(&db, "backlog_commit_b1_3");
	let first = write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	let second = write_src(&db, "a", 5, true);
	insert_level(&db, 2, "a");
	set_gate("backlog_commit_b1_3", true);
	settle(&db);

	assert!(first < second, "precondition: each row is its own source commit");
	assert!(price_commits_after(&db, first) < 2, "the producer must fold its backlog behind the lookup");
	assert_eq!(
		cost_rows(&db),
		vec![(Value::Int4(1), Some(Value::Int8(10))), (Value::Int4(2), Some(Value::Int8(15)))],
		"each left row must read the total as of its own source"
	);
}

fn pair_script(db: &TestDb, gate_name: &str, held: bool) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::src2 { k: utf8, v: int8 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::pair {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ \
		 FROM app::src APPLY gated_pass_through {{ gate: '{gate_name}' }} \
		 INNER JOIN {{ FROM app::src2 }} AS y USING (k, y.k) \
		 MAP {{ k, v: v + y_v }} }}"
	));
	create_cost(db, "app::pair");
	settle(db);
	if held {
		hold(db, gate_name);
	} else {
		set_gate(gate_name, true);
		write_src(db, "z", 0, true);
		settle(db);
	}
	let steps: [(&str, &str, i64, bool); 4] = [
		("app::src2", "a", 100, true),
		("app::src", "a", 1, true),
		("app::src2", "a", 200, false),
		("app::src", "a", 2, false),
	];
	for (id, (table, k, v, insert)) in steps.into_iter().enumerate() {
		write_table(db, table, k, v, insert);
		insert_level(db, id as i32 + 1, k);
		if !held {
			settle(db);
		}
	}
	set_gate(gate_name, true);
	settle(db);
}

#[test]
fn a_two_source_hash_join_with_a_backlog_pairs_each_source_as_per_version_did() {
	// The join keeps both sides in its own state, so one step over the backlog must pair each source as
	// separate steps did; a join run against the final state pairs the first rows with the last values.
	let backlogged = memory_db();
	pair_script(&backlogged, "backlog_commit_b1_4", true);
	let reference = memory_db();
	pair_script(&reference, "backlog_commit_b1_4_ref", false);

	let expected = vec![
		(Value::Int4(1), None),
		(Value::Int4(2), Some(Value::Int8(101))),
		(Value::Int4(3), Some(Value::Int8(201))),
		(Value::Int4(4), Some(Value::Int8(202))),
	];
	assert_eq!(cost_rows(&reference), expected, "precondition: the reference pairs each source with its own sum");
	assert_eq!(cost_rows(&backlogged), expected, "the backlog must pair exactly as the reference");
}

fn paced_left_script(db: &TestDb, gate_name: &str, read: &str, chained: bool) -> Vec<CommitVersion> {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin(
		"CREATE DEFERRED VIEW app::price { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { FROM app::src }",
	);
	db.admin("CREATE TABLE app::lsrc { k: utf8, v: int8 }");
	let gated = if chained {
		"app::left0"
	} else {
		"app::left"
	};
	db.admin(&format!("CREATE DEFERRED VIEW {gated} {{ k: utf8, v: int8 }} AS {{ \
		 FROM app::lsrc APPLY gated_pass_through {{ gate: '{gate_name}' }} }}"));
	if chained {
		db.admin("CREATE DEFERRED VIEW app::left { k: utf8, v: int8 } AS { FROM app::left0 }");
	}
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::cost {{ id: int8, usd: Option(int8) }} AS {{ FROM app::left {read} MAP {{ id: v, usd: p_v }} }}"
	));
	write_src(db, "a", 0, true);
	settle(db);
	set_gate(gate_name, false);
	write_table(db, "app::lsrc", "z", 0, true);
	assert!(
		poll_until(|| gate(gate_name).entered.then_some(()), SETTLE).is_some(),
		"the left producer never started the step that holds the backlog"
	);
	let written: Vec<CommitVersion> = (1..=ROUNDS as i64)
		.map(|round| {
			write_src(db, "a", round * 10, false);
			write_table(db, "app::lsrc", "a", round, true);
			db.engine().current_version().expect("the current version after a left write")
		})
		.collect();
	set_gate(gate_name, true);
	settle(db);
	written
}

fn left_commits_after(db: &TestDb, from: CommitVersion) -> usize {
	let frames = db.query("FROM system::views FILTER { name == 'left' }");
	let frame = frames.first().expect("system catalog frame");
	let ids = frame.column("id").expect("id reads").expect("system::views must expose an id column");
	let left = match ids.get_value(0) {
		Value::Uint8(id) => ObjectId::View(ViewId(id)),
		other => panic!("'left' has no numeric id in system::views, got {other:?}"),
	};
	db.commit_history()
		.iter()
		.filter(|entry| entry_changed_objects(entry).contains(&left))
		.filter(|entry| entry.version.source >= SourceVersion::from(from))
		.count()
}

fn assert_each_left_row_reads_its_own_price(db: &TestDb, written: &[CommitVersion]) {
	assert_backlog_formed(written);
	let expected: Vec<(Value, Option<Value>)> =
		(1..=ROUNDS as i64).map(|id| (Value::Int8(id), Some(Value::Int8(id * 10)))).collect();
	let rows: Vec<(Value, Option<Value>)> =
		cost_rows(db).into_iter().filter(|(id, _)| *id != Value::Int8(0)).collect();
	assert_eq!(rows, expected, "each left row must read the price written just before it, never a later one");
}

fn assert_the_left_producer_folded(db: &TestDb, written: &[CommitVersion]) {
	assert!(
		left_commits_after(db, written[0]) < ROUNDS,
		"the left producer must fold at least two versions into one commit"
	);
}

#[test]
fn a_view_on_the_left_of_an_emit_off_lookup_reads_each_left_row_at_its_own_source() {
	// A folded left view stamps every row with the last version, so the lookup hands early rows a later price.
	let db = memory_db();
	let written = paced_left_script(
		&db,
		"backlog_commit_l1",
		"LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: false }",
		false,
	);
	assert_each_left_row_reads_its_own_price(&db, &written);
	assert_the_left_producer_folded(&db, &written);
}

#[test]
fn a_view_on_the_left_of_a_latest_emit_off_join_reads_each_left_row_at_its_own_source() {
	// An emit-off latest join reads the right as of the left row, so a folded left view joins a later price.
	let db = memory_db();
	let written = paced_left_script(
		&db,
		"backlog_commit_l2",
		"LEFT JOIN { FROM app::price } AS p USING (k, p.k) WITH { emit: false, latest: true }",
		false,
	);
	assert_each_left_row_reads_its_own_price(&db, &written);
	assert_the_left_producer_folded(&db, &written);
}

#[test]
fn a_view_two_flows_above_the_left_of_an_emit_off_lookup_reads_each_left_row_at_its_own_source() {
	// A folded view above the left view hands it one change for the whole backlog, so every row reads the last
	// price.
	let db = memory_db();
	let written = paced_left_script(
		&db,
		"backlog_commit_l3",
		"LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: false }",
		true,
	);
	assert_each_left_row_reads_its_own_price(&db, &written);
}

#[test]
fn a_view_two_flows_above_the_right_of_an_emit_off_lookup_reads_each_left_row_at_its_own_source() {
	// A folded view above the looked up view skips the versions in between, so an early row reads a stale price.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_r1");
	db.admin(
		"CREATE DEFERRED VIEW app::price2 { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { FROM app::price }",
	);
	create_cost(&db, "app::price2");
	settle(&db);

	hold(&db, "backlog_commit_r1");
	let first = write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	let second = write_src(&db, "a", 20, false);
	insert_level(&db, 2, "a");
	set_gate("backlog_commit_r1", true);
	settle(&db);

	assert!(first < second, "precondition: each price is its own source commit");
	assert_eq!(
		cost_rows(&db),
		vec![(Value::Int4(1), Some(Value::Int8(10))), (Value::Int4(2), Some(Value::Int8(20)))],
		"each left row must read the price as of its own source through the chain"
	);
}

fn fused_member_of(db: &TestDb, member: &str, head: &str) -> bool {
	let flow_id = |name: &str| -> u64 {
		let ids: Vec<u64> = db
			.query(&format!("FROM system::flow::flows FILTER {{ name == '{name}' }}"))
			.iter()
			.flat_map(|frame| frame.rows())
			.map(|row| row.get::<u64>("id").expect("the id column reads").expect("an id"))
			.collect();
		assert_eq!(ids.len(), 1, "'{name}' must name exactly one flow: {ids:?}");
		ids[0]
	};
	let (member, head) = (flow_id(member), flow_id(head));
	poll_until(
		|| {
			db.query("FROM system::flow::units")
				.iter()
				.flat_map(|frame| frame.rows())
				.any(|row| {
					row.get::<u64>("flow_id").expect("flow_id reads") == Some(member)
						&& row.get::<u64>("unit").expect("unit reads") == Some(head)
				})
				.then_some(())
		},
		SETTLE,
	)
	.is_some()
}

#[test]
fn a_lookup_fused_under_its_left_view_reads_each_left_row_at_its_own_source() {
	// A fold feeds a fused member one change at the last source, so every left row would read the last price.
	let db = memory_db();
	let gate_name = "backlog_commit_u1";
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin(
		"CREATE DEFERRED VIEW app::price { k: utf8, v: int8 } WITH { partition: { by: { k } } } AS { FROM app::src }",
	);
	db.admin("CREATE TABLE app::lsrc { k: utf8, v: int8 }");
	db.admin(&format!("CREATE DEFERRED VIEW app::left {{ k: utf8, v: int8 }} AS {{ \
		 FROM app::lsrc APPLY gated_pass_through {{ gate: '{gate_name}' }} }}"));
	db.admin("CREATE DEFERRED VIEW app::cost { id: int8, usd: Option(int8) } AS { FROM app::left \
		 LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: false } MAP { id: v, usd: p_v } }");
	write_src(&db, "a", 0, true);
	settle(&db);
	assert!(fused_member_of(&db, "cost", "left"), "precondition: the lookup must run fused under its left view");

	set_gate(gate_name, false);
	write_table(&db, "app::lsrc", "z", 0, true);
	assert!(
		poll_until(|| gate(gate_name).entered.then_some(()), SETTLE).is_some(),
		"the unit never started the step that holds the backlog"
	);
	let written: Vec<CommitVersion> = (1..=ROUNDS as i64)
		.map(|round| {
			write_src(&db, "a", round * 10, false);
			write_table(&db, "app::lsrc", "a", round, true);
			db.engine().current_version().expect("the current version after a left write")
		})
		.collect();
	set_gate(gate_name, true);
	settle(&db);

	assert_backlog_formed(&written);
	let expected: Vec<(Value, Option<Value>)> =
		(1..=ROUNDS as i64).map(|id| (Value::Int8(id), Some(Value::Int8(id * 10)))).collect();
	let rows: Vec<(Value, Option<Value>)> =
		cost_rows(&db).into_iter().filter(|(id, _)| *id != Value::Int8(0)).collect();
	assert_eq!(rows, expected, "each left row must read the price written just before it, never a later one");
}

#[test]
fn a_lookup_reader_takes_a_layered_backlog_from_below_its_cursor_and_keeps_the_history_it_reads() {
	// A lease floor at the reader's cursor sits above the early layers, so their reads land on gone history or a
	// later version.
	let db = memory_db();
	create_price(&db, "app::src", "backlog_commit_f1");
	create_level(&db);
	db.admin("CREATE DEFERRED VIEW app::cost { id: int4, usd: Option(int8) } AS { \
		 FROM app::level \
		 LEFT LOOKUP { FROM app::price } AS p USING (k, p.k) WITH { emit: 1s, retention: { left: 10s } } \
		 MAP { id, usd: p_v } }");
	insert_level(&db, 1, "a");
	settle(&db);

	let written = backlog(&db, "backlog_commit_f1", ROUNDS);
	assert_backlog_formed(&written);

	assert_eq!(
		poll_until(
			|| {
				let rows = cost_rows(&db);
				(rows == vec![(Value::Int4(1), Some(Value::Int8(ROUNDS as i64 - 1)))]).then_some(rows)
			},
			SETTLE
		),
		Some(vec![(Value::Int4(1), Some(Value::Int8(ROUNDS as i64 - 1)))]),
		"the left row must end on the newest price once the folded backlog is read"
	);
}

#[test]
fn a_re_emitting_lookup_matches_every_left_row_when_its_right_view_folds_a_backlog() {
	// A right layer read after the left row it should match must still re-emit that row, otherwise the row keeps
	// none.
	let db = memory_db();
	let gate_name = "backlog_commit_r2";
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t1 { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE DEFERRED VIEW app::a { k: utf8, v: int8 } AS { FROM app::t1 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::b {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ FROM app::src APPLY gated_pass_through {{ gate: '{gate_name}' }} }}"
	));
	db.admin("CREATE DEFERRED VIEW app::cost { id: int8, usd: Option(int8) } AS { \
		 FROM app::a LEFT LOOKUP { FROM app::b } AS p USING (k, p.k) WITH { emit: true } MAP { id: v, usd: p_v } }");
	settle(&db);

	hold(&db, gate_name);
	for round in 1..=ROUNDS as i64 {
		let k = format!("k{round}");
		write_table(&db, "app::src", &k, round * 10, true);
		write_table(&db, "app::t1", &k, round, true);
	}
	set_gate(gate_name, true);
	settle(&db);

	let expected: Vec<(Value, Option<Value>)> =
		(1..=ROUNDS as i64).map(|id| (Value::Int8(id), Some(Value::Int8(id * 10)))).collect();
	assert_eq!(cost_rows(&db), expected, "every left row must end matched to the right row written before it");
}

fn view_commits_after(db: &TestDb, name: &str, from: CommitVersion) -> usize {
	let frames = db.query(&format!("FROM system::views FILTER {{ name == '{name}' }}"));
	let frame = frames.first().expect("system catalog frame");
	let ids = frame.column("id").expect("id reads").expect("system::views must expose an id column");
	let view = match ids.get_value(0) {
		Value::Uint8(id) => ObjectId::View(ViewId(id)),
		other => panic!("'{name}' has no numeric id in system::views, got {other:?}"),
	};
	db.commit_history()
		.iter()
		.filter(|entry| entry_changed_objects(entry).contains(&view))
		.filter(|entry| entry.version.source >= SourceVersion::from(from))
		.count()
}

#[test]
fn a_latest_emit_off_join_matches_every_left_row_when_its_right_view_folds_a_backlog() {
	// A folded right view must still hand the join each source in order, otherwise an early left row joins a later
	// right value or none.
	let db = memory_db();
	let gate_name = "backlog_commit_j2";
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t1 { k: utf8, v: int8 }");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin("CREATE DEFERRED VIEW app::a { k: utf8, v: int8 } AS { FROM app::t1 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::b {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ FROM app::src APPLY gated_pass_through {{ gate: '{gate_name}' }} }}"
	));
	db.admin("CREATE DEFERRED VIEW app::cost { id: int8, usd: Option(int8) } AS { \
		 FROM app::a LEFT JOIN { FROM app::b } AS p USING (k, p.k) WITH { emit: false, latest: true } MAP { id: v, usd: p_v } }");
	settle(&db);

	hold(&db, gate_name);
	let from = db.engine().current_version().expect("the current version before the backlog");
	for round in 1..=ROUNDS as i64 {
		let k = format!("k{round}");
		write_table(&db, "app::src", &k, round * 10, true);
		write_table(&db, "app::t1", &k, round, true);
	}
	set_gate(gate_name, true);
	settle(&db);

	let expected: Vec<(Value, Option<Value>)> =
		(1..=ROUNDS as i64).map(|id| (Value::Int8(id), Some(Value::Int8(id * 10)))).collect();
	assert_eq!(cost_rows(&db), expected, "every left row must end matched to the right row written before it");
	assert!(view_commits_after(&db, "b", from) < ROUNDS, "the right producer of a join must fold its backlog");
}

#[test]
fn an_emit_off_lookup_reads_each_source_inside_its_folded_right_view() {
	// A lookup that reads the folded commit's end state pairs early left rows with a later price; one that refuses
	// the fold keeps its producer per version.
	let db = memory_db();
	let gate_name = "backlog_commit_x1";
	create_price(&db, "app::src", gate_name);
	create_cost(&db, "app::price");
	settle(&db);

	hold(&db, gate_name);
	let from = write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	write_src(&db, "a", 20, false);
	insert_level(&db, 2, "a");
	write_src(&db, "a", 30, false);
	insert_level(&db, 3, "a");
	db.command("DELETE app::src FILTER { k == 'a' }");
	insert_level(&db, 4, "a");
	set_gate(gate_name, true);
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![
			(Value::Int4(1), Some(Value::Int8(10))),
			(Value::Int4(2), Some(Value::Int8(20))),
			(Value::Int4(3), Some(Value::Int8(30))),
			(Value::Int4(4), None),
		],
		"each left row must read the price as of its own source, and a removed price must read as none"
	);
	assert!(price_commits_after(&db, from) < 4, "the right producer must fold its backlog behind the lookup");
}

#[test]
fn an_emit_off_lookup_reads_each_source_inside_its_folded_right_view_without_a_commit_log() {
	// A lookup that reads the folded commit's end state pairs early left rows with a later price; one that refuses
	// the fold keeps its producer per version.
	let db = memory_db_without_commit_log();
	let gate_name = "backlog_commit_x2";
	create_price(&db, "app::src", gate_name);
	create_cost(&db, "app::price");
	settle(&db);

	hold(&db, gate_name);
	let from = write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	write_src(&db, "a", 20, false);
	insert_level(&db, 2, "a");
	write_src(&db, "a", 30, false);
	insert_level(&db, 3, "a");
	db.command("DELETE app::src FILTER { k == 'a' }");
	insert_level(&db, 4, "a");
	set_gate(gate_name, true);
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![
			(Value::Int4(1), Some(Value::Int8(10))),
			(Value::Int4(2), Some(Value::Int8(20))),
			(Value::Int4(3), Some(Value::Int8(30))),
			(Value::Int4(4), None),
		],
		"each left row must read the price as of its own source, and a removed price must read as none"
	);
	assert!(price_commits_after(&db, from) < 4, "the right producer must fold its backlog behind the lookup");
}

#[test]
fn a_lookup_reads_a_folded_commit_whose_only_output_came_before_the_left_row() {
	// A fold whose one output sits below a later quiet source must still answer the left row between them,
	// otherwise it reads the commit before and finds no price.
	let db = memory_db();
	let gate_name = "backlog_commit_x3";
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::src { k: utf8, v: int8 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::price {{ k: utf8, v: int8 }} WITH {{ partition: {{ by: {{ k }} }} }} AS {{ \
		 FROM app::src APPLY gated_pass_through {{ gate: '{gate_name}' }} FILTER {{ k == 'a' }} }}"
	));
	create_cost(&db, "app::price");
	settle(&db);

	hold(&db, gate_name);
	write_src(&db, "a", 10, true);
	insert_level(&db, 1, "a");
	write_src(&db, "b", 1, true);
	set_gate(gate_name, true);
	settle(&db);

	assert_eq!(
		cost_rows(&db),
		vec![(Value::Int4(1), Some(Value::Int8(10)))],
		"the left row must read the price written just before it, even when the fold ends on a source with no output"
	);
}
