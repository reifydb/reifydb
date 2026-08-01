// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().unwrap())
}

fn row_count(db: &TestDb, rql: &str) -> usize {
	let frames = db.command(rql);
	frames.first().map(|f| f.row_count()).unwrap_or(0)
}

#[test]
fn skipping_producer_still_updates_shared_consumer() {
	// A producer with no relevant change in a commit must still settle and unblock its consumers;
	// otherwise a view that also depends on a changed producer is never dispatched and goes
	// silently stale.
	let mut db = setup();
	db.admin("create namespace test");
	db.admin("create table test::t1 { id: int4, name: utf8 }");
	db.admin("create table test::t2 { id: int4, name: utf8 }");
	// Two single-source views feed one append view, so the consumer flow c has two producers.
	db.admin("create view test::a { id: int4, name: utf8 } as { from test::t1 }");
	db.admin("create view test::b { id: int4, name: utf8 } as { from test::t2 }");
	db.admin("create view test::c { id: int4, name: utf8 } as { from test::a append { from test::b } }");

	// Commit 1 touches both tables: flows a and b both emit, c aggregates both branches.
	db.command(r#"INSERT test::t1 [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);
	db.command(r#"INSERT test::t2 [{ id: 3, name: "Charlie" }]"#);
	assert_eq!(row_count(&db, "from test::c"), 3, "c must contain both branches after both tables are seeded");

	// Commit 2 touches only t1: flow a re-runs, flow b has no relevant change and skips.
	// Because c depends on both a and b, c can only re-run if the skipping b still
	// decremented c's in_degree to zero.
	db.command(r#"UPDATE test::t1 { id, name: "Alicia" } FILTER { id == 1 }"#);

	assert_eq!(
		row_count(&db, r#"from test::c filter { name == "Alicia" }"#),
		1,
		"c did not reflect the t1-only update: a skipping producer (b) failed to unblock the shared consumer (c)"
	);
	assert_eq!(
		row_count(&db, r#"from test::c filter { name == "Alice" }"#),
		0,
		"the pre-update value must be gone from c once the update propagated through a"
	);
	assert_eq!(
		row_count(&db, r#"from test::c filter { name == "Charlie" }"#),
		1,
		"b's branch must remain in c even though b skipped this commit"
	);
	assert_eq!(row_count(&db, "from test::c"), 3, "c row count must be stable across the update");

	db.stop();
}
