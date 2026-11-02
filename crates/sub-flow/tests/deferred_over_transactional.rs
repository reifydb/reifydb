// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_value::{
	params::Params,
	value::{duration::Duration, frame::frame::Frame, identity::IdentityId},
};

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE ns");
	db.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	db.admin("CREATE TRANSACTIONAL VIEW ns::t { id: int4, v: int4 } AS { FROM ns::src | filter { v > 10 } }");
	db.admin("CREATE DEFERRED VIEW ns::d { id: int4, v: int4 } AS { FROM ns::t | filter { v < 90 } }");
	db
}

fn pairs(frames: &[Frame]) -> Vec<(i32, i32)> {
	let mut pairs: Vec<(i32, i32)> = frames
		.iter()
		.flat_map(|frame| frame.rows())
		.map(|row| (row.get::<i32>("id").unwrap().unwrap(), row.get::<i32>("v").unwrap().unwrap()))
		.collect();
	pairs.sort();
	pairs
}

fn await_pairs(db: &TestDb, rql: &str, want: Vec<(i32, i32)>) -> Vec<(i32, i32)> {
	await_value(want, TIMEOUT, || pairs(&db.query(rql)))
}

fn run_in_txn(db: &TestDb, statements: &[&str], commit: bool) {
	let mut txn = db.engine().begin_command(IdentityId::root()).expect("begin command");
	for rql in statements {
		let r = txn.rql(rql, Params::None);
		assert!(r.error.is_none(), "{rql} failed: {:?}", r.error);
	}
	if commit {
		txn.commit().expect("commit");
	} else {
		txn.rollback().expect("rollback");
	}
}

#[test]
fn a_deferred_view_over_a_transactional_view_follows_insert_update_delete() {
	// The deferred hop sees t only through CDC; a lost, stale or reordered view diff shows here.
	let db = setup();
	for (rql, want) in [
		("INSERT ns::src [{ id: 1, v: 5 }, { id: 2, v: 50 }, { id: 3, v: 95 }]", vec![(2, 50)]),
		("UPDATE ns::src { v: 60 } FILTER { id == 2 }", vec![(2, 60)]),
		("DELETE ns::src FILTER { id == 2 }", vec![]),
	] {
		db.command(rql);
		assert_eq!(await_pairs(&db, "FROM ns::d", want.clone()), want, "d after: {rql}");
	}
}

#[test]
fn a_rolled_back_write_never_reaches_the_deferred_reader() {
	// A rollback drops t's rows with the source rows, so no CDC may carry them to d.
	let db = setup();
	run_in_txn(&db, &["INSERT ns::src [{ id: 2, v: 50 }]"], false);
	db.command("INSERT ns::src [{ id: 4, v: 40 }]");
	assert_eq!(await_pairs(&db, "FROM ns::d", vec![(4, 40)]), vec![(4, 40)]);
}

#[test]
fn a_change_made_twice_in_one_txn_reaches_the_deferred_reader_once() {
	// d must see the committed end state, not the insert and the update as two rows.
	let db = setup();
	run_in_txn(&db, &["INSERT ns::src [{ id: 2, v: 50 }]", "UPDATE ns::src { v: 60 } FILTER { id == 2 }"], true);
	assert_eq!(await_pairs(&db, "FROM ns::d", vec![(2, 60)]), vec![(2, 60)]);
}

#[test]
fn a_deferred_view_reads_the_end_of_a_two_hop_transactional_chain() {
	// Only the last transactional hop feeds d2; it must carry what both hops did in the txn.
	let db = setup();
	db.admin("CREATE TRANSACTIONAL VIEW ns::t2 { id: int4, v: int4 } AS { FROM ns::t | filter { v < 90 } }");
	db.admin("CREATE DEFERRED VIEW ns::d2 { id: int4, v: int4 } AS { FROM ns::t2 }");
	for rql in [
		"INSERT ns::src [{ id: 1, v: 5 }, { id: 2, v: 50 }, { id: 3, v: 95 }]",
		"UPDATE ns::src { v: 70 } FILTER { id == 1 }",
		"UPDATE ns::src { v: 99 } FILTER { id == 2 }",
	] {
		db.command(rql);
		let want = pairs(&db.query("FROM ns::t2"));
		assert_eq!(await_pairs(&db, "FROM ns::d2", want.clone()), want, "d2 after: {rql}");
	}
}

#[test]
fn a_deferred_reader_resolves_a_transactional_dictionary_view() {
	// CDC carries t's stored dictionary ids, so d must resolve them back to text, never show the ids.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE ns");
	db.admin("CREATE DICTIONARY ns::syms FOR utf8 AS uint4");
	db.admin("CREATE TABLE ns::src { id: int4, sym: utf8 with { dictionary: ns::syms } }");
	db.admin(
		"CREATE TRANSACTIONAL VIEW ns::t { id: int4, sym: utf8 with { dictionary: ns::syms } } AS { FROM ns::src }",
	);
	db.admin("CREATE DEFERRED VIEW ns::d { id: int4, sym: utf8 } AS { FROM ns::t }");
	let syms = || -> Vec<String> {
		let mut syms: Vec<String> = db
			.query("FROM ns::d")
			.iter()
			.flat_map(|frame| frame.rows())
			.map(|row| row.get::<String>("sym").unwrap().unwrap())
			.collect();
		syms.sort();
		syms
	};
	for (rql, want) in [
		("INSERT ns::src [{ id: 1, sym: 'red' }, { id: 2, sym: 'green' }]", vec!["green", "red"]),
		("UPDATE ns::src { sym: 'blue' } FILTER { id == 1 }", vec!["blue", "green"]),
		("DELETE ns::src FILTER { id == 2 }", vec!["blue"]),
	] {
		db.command(rql);
		let want: Vec<String> = want.into_iter().map(String::from).collect();
		assert_eq!(await_value(want.clone(), TIMEOUT, syms), want, "d after: {rql}");
	}
}

#[test]
fn a_sort_value_change_upstream_reaches_the_deferred_reader_as_one_row() {
	// The sort value moves t's key; d must replace the row, not keep the old one or lose it.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE ns");
	db.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	db.admin("CREATE TRANSACTIONAL VIEW ns::t { id: int4, v: int4 } AS { FROM ns::src | sort { v } }");
	db.admin("CREATE DEFERRED VIEW ns::d { id: int4, v: int4 } AS { FROM ns::t }");
	db.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	assert_eq!(await_pairs(&db, "FROM ns::d", vec![(1, 10), (2, 20)]), vec![(1, 10), (2, 20)]);
	db.command("UPDATE ns::src { v: 90 } FILTER { id == 1 }");
	assert_eq!(await_pairs(&db, "FROM ns::d", vec![(1, 90), (2, 20)]), vec![(1, 90), (2, 20)]);
}

#[test]
fn a_deferred_aggregate_over_a_transactional_view_counts_right() {
	// A stateful reader keeps counts across commits; a lost retraction or a double insert skews them for good.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW ns::n { v: int4, n: int8 } AS { FROM ns::t | aggregate { n: math::count(id) } by { v } }",
	);
	let counts = || -> Vec<(i32, i64)> {
		let mut counts: Vec<(i32, i64)> = db
			.query("FROM ns::n")
			.iter()
			.flat_map(|frame| frame.rows())
			.map(|row| (row.get::<i32>("v").unwrap().unwrap(), row.get::<i64>("n").unwrap().unwrap()))
			.collect();
		counts.sort();
		counts
	};
	for (rql, want) in [
		("INSERT ns::src [{ id: 1, v: 50 }, { id: 2, v: 50 }, { id: 3, v: 60 }]", vec![(50, 2), (60, 1)]),
		("UPDATE ns::src { v: 60 } FILTER { id == 1 }", vec![(50, 1), (60, 2)]),
		("DELETE ns::src FILTER { id == 2 }", vec![(60, 2)]),
		("UPDATE ns::src { v: 5 } FILTER { id == 3 }", vec![(60, 1)]),
	] {
		db.command(rql);
		assert_eq!(await_value(want.clone(), TIMEOUT, counts), want, "n after: {rql}");
	}
}
