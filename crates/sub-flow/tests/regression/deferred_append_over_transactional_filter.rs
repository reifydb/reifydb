// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A deferred consumer of a transactional source view registered for that view's own storage sources
// receives base-table changes on a pass-through node, leaking raw base rows into the APPEND's
// filter branch. The all-transactional twin reads its source synchronously and cannot.

use std::time::Duration as StdDuration;

use reifydb::{Value, WithSubsystem, embedded};
use reifydb_test_harness::db::{TestDb, await_value};

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().expect("build memory db with flow"))
}

// Sorted multiset of (id, cat) over the user columns, so comparison is order-insensitive.
fn rows(db: &TestDb, rql: &str) -> Vec<(i32, i32)> {
	let frames = db.query(rql);
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

fn await_rows(db: &TestDb, rql: &str, want: usize) -> Vec<(i32, i32)> {
	// Returns the last observation even on timeout, so the caller's assertion can report the
	// actual leaked contents rather than only a count.
	let mut last = Vec::new();
	await_value(want, StdDuration::from_secs(10), || {
		last = rows(db, rql);
		last.len()
	});
	last
}

#[test]
fn deferred_append_over_transactional_filter_does_not_leak_base() {
	let db = setup();
	db.admin("CREATE NAMESPACE v");
	db.admin("CREATE TABLE v::base { id: int4, cat: int4 }");
	db.admin(
		"CREATE TRANSACTIONAL VIEW v::txf { id: int4, cat: int4 } AS { FROM v::base FILTER cat == 1 MAP { id, cat } }",
	);
	db.admin(
		"CREATE DEFERRED VIEW v::du { id: int4, cat: int4 } AS { FROM v::base APPEND { FROM v::txf } MAP { id, cat } }",
	);
	db.admin(
		"CREATE TRANSACTIONAL VIEW v::tu { id: int4, cat: int4 } AS { FROM v::base APPEND { FROM v::txf } MAP { id, cat } }",
	);

	// Pure inserts mask the leak: a leaked base row for a filter-matching id collides on identity
	// with the legitimate filtered row. The UPDATE moves a row out of the filter and the DELETE
	// breaks the collision, which is what surfaces a leaked non-matching row.
	db.command("INSERT v::base [{ id: 1, cat: 1 }]");
	db.command("INSERT v::base [{ id: 2, cat: 2 }]");
	db.command("INSERT v::base [{ id: 3, cat: 1 }]");
	db.command("INSERT v::base [{ id: 4, cat: 2 }]");
	db.command("UPDATE v::base { cat: 9 } FILTER id == 1");
	db.command("DELETE v::base FILTER id == 4");

	// The all-transactional twin is the synchronous ground truth: base plus the cat==1 rows.
	let twin = rows(&db, "FROM v::tu");
	let deferred = await_rows(&db, "FROM v::du", twin.len());
	assert_eq!(
		deferred, twin,
		"deferred APPEND(base, transactional-filter) must equal the all-transactional twin; a larger \
		 deferred multiset means base rows leaked into the transactional-filter branch \
		 (twin={twin:?} deferred={deferred:?})"
	);
}
