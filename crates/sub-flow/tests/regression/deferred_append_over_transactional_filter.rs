// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Reproducer: a DEFERRED view that APPENDs a base table with a TRANSACTIONAL filter view leaks the
// unfiltered base rows into the transactional-filter branch.
//
// WHY: a deferred consumer of a transactional source view is (over-)registered for the source view's
// own primitive sources (the base table) in `register_source_view`. The deferred dispatch
// (`seed_entry_nodes`) then routes base-table changes to the consumer's `SourceView` node, which is a
// pass-through, so the raw base rows flow into the APPEND's transactional-view branch instead of only
// the filter's materialized output. The all-transactional twin is immune (its source view is read
// synchronously), so the deferred view must hold the SAME multiset as the twin.
//
// The leak is masked for pure inserts (leaked base rows for filter-matching ids collide on identity
// with the legitimate filtered rows and cancel out), so the workload includes an UPDATE that moves a
// row OUT of the filter and a DELETE - which break the masking and surface a leaked NON-matching row.
//
// RED until the over-registration is fixed (the consumer's `SourceView` node should be registered only
// for the transactional view's OUTPUT, not its base sources); GREEN after.

use std::{
	thread,
	time::{Duration, Instant},
};

use reifydb::{Database, Params, Value, WithSubsystem, embedded};

fn setup() -> Database {
	embedded::memory().with_flow(|c| c).build().expect("build memory db with flow")
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn command(db: &Database, rql: &str) {
	db.command_as_root(rql, Params::None).unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"));
}

// Sorted multiset of (id, cat) over the user columns, so comparison is order-insensitive.
fn rows(db: &Database, rql: &str) -> Vec<(i32, i32)> {
	let frames = db.query_as_root(rql, Params::None).unwrap_or_else(|e| panic!("query failed: {e:?}\nrql: {rql}"));
	let mut out = Vec::new();
	for f in &frames {
		for row in f.to_rows() {
			let mut id = None;
			let mut cat = None;
			for (name, value) in row {
				match (name.as_str(), value) {
					("id", Value::Int4(v)) => id = Some(v),
					("cat", Value::Int4(v)) => cat = Some(v),
					_ => {}
				}
			}
			out.push((id.expect("id column"), cat.expect("cat column")));
		}
	}
	out.sort_unstable();
	out
}

// Poll a deferred view until it holds `want` rows or the deadline passes, then return its sorted
// multiset so the caller's assertion reports the actual (possibly leaked) contents.
fn await_rows(db: &Database, rql: &str, want: usize) -> Vec<(i32, i32)> {
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		let got = rows(db, rql);
		if got.len() == want || Instant::now() >= deadline {
			return got;
		}
		thread::sleep(Duration::from_millis(20));
	}
}

#[test]
fn deferred_append_over_transactional_filter_does_not_leak_base() {
	let db = setup();
	admin(&db, "CREATE NAMESPACE v");
	admin(&db, "CREATE TABLE v::base { id: int4, cat: int4 }");
	admin(
		&db,
		"CREATE TRANSACTIONAL VIEW v::txf { id: int4, cat: int4 } AS { FROM v::base FILTER cat == 1 MAP { id, cat } }",
	);
	admin(
		&db,
		"CREATE DEFERRED VIEW v::du { id: int4, cat: int4 } AS { FROM v::base APPEND { FROM v::txf } MAP { id, cat } }",
	);
	admin(
		&db,
		"CREATE TRANSACTIONAL VIEW v::tu { id: int4, cat: int4 } AS { FROM v::base APPEND { FROM v::txf } MAP { id, cat } }",
	);

	command(&db, "INSERT v::base [{ id: 1, cat: 1 }]");
	command(&db, "INSERT v::base [{ id: 2, cat: 2 }]");
	command(&db, "INSERT v::base [{ id: 3, cat: 1 }]");
	command(&db, "INSERT v::base [{ id: 4, cat: 2 }]");
	command(&db, "UPDATE v::base { cat: 9 } FILTER id == 1");
	command(&db, "DELETE v::base FILTER id == 4");

	// All-transactional twin is the synchronous ground truth: base ∪ (cat==1 rows).
	let twin = rows(&db, "FROM v::tu");
	let deferred = await_rows(&db, "FROM v::du", twin.len());
	assert_eq!(
		deferred, twin,
		"deferred APPEND(base, transactional-filter) must equal the all-transactional twin; a larger \
		 deferred multiset means base rows leaked into the transactional-filter branch \
		 (twin={twin:?} deferred={deferred:?})"
	);
}
