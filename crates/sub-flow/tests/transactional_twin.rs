// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_value::value::{Value, duration::Duration};

const TIMEOUT: Duration = Duration::from_seconds_const(5);

const COLUMNS: &str = "id: int4, sym: utf8, v: int4";

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE tw");
	db.admin("CREATE DICTIONARY tw::syms FOR utf8 AS uint4");
	db.admin("CREATE TABLE tw::src { id: int4, sym: utf8, v: int4 }");
	db.admin("CREATE TABLE tw::src2 { id: int4, sym: utf8, v: int4 }");
	db.admin("CREATE TABLE tw::dsrc { id: int4, sym: utf8 with { dictionary: tw::syms }, v: int4 }");
	db
}

fn dml(table: &str) -> Vec<String> {
	vec![
		format!(
			"INSERT {table} [{{ id: 1, v: 5, sym: 'a' }}, {{ id: 2, v: 50, sym: 'b' }}, {{ id: 3, v: 95, sym: 'c' }}]"
		),
		format!("UPDATE {table} {{ v: 60 }} FILTER {{ id == 1 }}"),
		format!("UPDATE {table} {{ v: 5 }} FILTER {{ id == 2 }}"),
		format!("DELETE {table} FILTER {{ id == 3 }}"),
		format!(
			"INSERT {table} [{{ id: 4, v: 40, sym: 'd' }}]; UPDATE {table} {{ v: 70 }} FILTER {{ id == 4 }}"
		),
	]
}

fn render(cells: Vec<(String, Value)>) -> String {
	assert!(cells.iter().any(|(name, _)| name == "#rownum"), "a view row came back without #rownum: {cells:?}");
	let mut cells: Vec<String> = cells
		.into_iter()
		.filter(|(name, _)| name == "#rownum" || !name.starts_with('#'))
		.map(|(name, value)| format!("{name}={value}"))
		.collect();
	cells.sort();
	cells.join(",")
}

fn rows(db: &TestDb, view: &str) -> Vec<String> {
	let mut rows: Vec<String> =
		db.query(&format!("FROM {view}")).iter().flat_map(|frame| frame.to_rows()).map(render).collect();
	rows.sort();
	rows
}

fn check_twins(db: &TestDb, statements: &[String], transactional: &str, deferred: &str) {
	for rql in statements {
		db.command(rql);
		assert!(db.await_all_flows(TIMEOUT), "deferred flows did not catch up after: {rql}");
		let want = rows(db, transactional);
		let got = await_value(want.clone(), TIMEOUT, || rows(db, deferred));
		assert_eq!(got, want, "{deferred} diverged from {transactional} after: {rql}");
	}
}

fn twin(columns: &str, body: &str, statements: &[String]) {
	let db = setup();
	db.admin(&format!("CREATE TRANSACTIONAL VIEW tw::t {{ {columns} }} AS {{ {body} }}"));
	db.admin(&format!("CREATE DEFERRED VIEW tw::d {{ {columns} }} AS {{ {body} }}"));
	check_twins(&db, statements, "tw::t", "tw::d");
}

#[test]
fn twin_filter() {
	// Both engines must agree on which rows cross the filter, in both directions.
	twin(COLUMNS, "FROM tw::src | filter { v > 50 }", &dml("tw::src"));
}

#[test]
fn twin_map() {
	// A projection must keep the same rows and row numbers in both engines.
	twin("id: int4, v: int4", "FROM tw::src | map { id, v }", &dml("tw::src"));
}

#[test]
fn twin_extend() {
	// The computed column must be recomputed on every update, the same way in both engines.
	twin("id: int4, sym: utf8, v: int4, w: int4", "FROM tw::src | extend { w: v + 1 }", &dml("tw::src"));
}

#[test]
fn twin_filter_map() {
	// A filter on a column the map then drops must still retract by the old value in both engines.
	twin("id: int4, sym: utf8", "FROM tw::src | filter { v < 50 } | map { id, sym }", &dml("tw::src"));
}

#[test]
fn twin_append() {
	// Both engines must stamp the same lane row numbers on the two merged inputs.
	let statements: Vec<String> = dml("tw::src").into_iter().chain(dml("tw::src2")).collect();
	twin(COLUMNS, "FROM tw::src | append { FROM tw::src2 }", &statements);
}

#[test]
fn twin_sorted() {
	// A sort value change moves the row's key; both engines must end with one row per id.
	twin(COLUMNS, "FROM tw::src | filter { v > 20 } | sort { v }", &dml("tw::src"));
}

#[test]
fn twin_dictionary() {
	// Both engines must resolve the source's dictionary ids to the same text.
	twin(COLUMNS, "FROM tw::dsrc", &dml("tw::dsrc"));
}

#[test]
fn twin_chain() {
	// Two hops each side; the second hop sees only the first hop's output in both engines.
	let db = setup();
	for (kind, first, second) in [("TRANSACTIONAL", "tw::t1", "tw::t2"), ("DEFERRED", "tw::d1", "tw::d2")] {
		db.admin(&format!(
			"CREATE {kind} VIEW {first} {{ {COLUMNS} }} AS {{ FROM tw::src | filter {{ v > 10 }} }}"
		));
		db.admin(&format!(
			"CREATE {kind} VIEW {second} {{ id: int4, v: int4 }} AS {{ FROM {first} | filter {{ v < 90 }} | map {{ id, v }} }}"
		));
	}
	check_twins(&db, &dml("tw::src"), "tw::t2", "tw::d2");
}
