// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_value::value::{duration::Duration, frame::frame::Frame};

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE ns");
	db.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
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

fn follows_the_source(up: &str, down: &str) {
	let db = setup();
	db.admin(&format!(
		"CREATE {up} VIEW ns::up {{ id: int4, v: int4 }} AS {{ FROM ns::src | filter {{ v > 10 }} }}"
	));
	db.admin(&format!(
		"CREATE {down} VIEW ns::down {{ id: int4, v: int4 }} AS {{ FROM ns::up | filter {{ v < 90 }} }}"
	));
	for (rql, want) in [
		("INSERT ns::src [{ id: 1, v: 5 }, { id: 2, v: 50 }, { id: 3, v: 95 }]", vec![(2, 50)]),
		("UPDATE ns::src { v: 60 } FILTER { id == 2 }", vec![(2, 60)]),
		("UPDATE ns::src { v: 70 } FILTER { id == 1 }", vec![(1, 70), (2, 60)]),
		("UPDATE ns::src { v: 95 } FILTER { id == 2 }", vec![(1, 70)]),
		("DELETE ns::src FILTER { id == 1 }", vec![]),
	] {
		db.command(rql);
		let got = await_value(want.clone(), TIMEOUT, || pairs(&db.query("FROM ns::down")));
		assert_eq!(got, want, "{down} view over {up} view diverged after: {rql}");
	}
}

#[test]
fn a_transactional_view_over_a_transactional_view_follows_the_source() {
	// Both hops run inside the writing txn, so the second hop must be right with no wait at all.
	follows_the_source("TRANSACTIONAL", "TRANSACTIONAL");
}

#[test]
fn a_deferred_view_over_a_transactional_view_follows_the_source() {
	// The deferred hop sees the transactional view only through its CDC; a missed or reordered diff shows here.
	follows_the_source("TRANSACTIONAL", "DEFERRED");
}

#[test]
fn a_deferred_view_over_a_deferred_view_follows_the_source() {
	// The baseline the other kinds are held to; if this one fails, the chain machinery itself is broken.
	follows_the_source("DEFERRED", "DEFERRED");
}

#[test]
fn a_transactional_view_over_a_deferred_view_is_refused_with_flow_085() {
	// A deferred view changes after commit, so a transactional reader of it could never be current.
	let db = setup();
	db.admin("CREATE DEFERRED VIEW ns::up { id: int4, v: int4 } AS { FROM ns::src }");

	let Err(err) = db.try_admin("CREATE TRANSACTIONAL VIEW ns::down { id: int4, v: int4 } AS { FROM ns::up }")
	else {
		panic!("a transactional view over a deferred view must be refused, but the create succeeded");
	};
	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_085", "got: {diagnostic:?}");
	assert!(db.try_query("FROM ns::down").is_err(), "a refused create must not leave a readable view");
}

#[test]
fn a_transactional_view_appending_a_deferred_view_is_refused_with_flow_085() {
	// The check must cover every source of the flow, not only the first one.
	let db = setup();
	db.admin("CREATE DEFERRED VIEW ns::up { id: int4, v: int4 } AS { FROM ns::src }");

	let Err(err) = db.try_admin(
		"CREATE TRANSACTIONAL VIEW ns::down { id: int4, v: int4 } AS { FROM ns::src | append { FROM ns::up } }",
	) else {
		panic!("a transactional view appending a deferred view must be refused, but the create succeeded");
	};
	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_085", "got: {diagnostic:?}");
	assert!(db.try_query("FROM ns::down").is_err(), "a refused create must not leave a readable view");
}
