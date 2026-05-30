// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression: transactional sub-flow execution is dataflow-scheduled - each view runs
// the moment all its direct producer flows settle, instead of waiting for a whole
// topological level to finish. The subtle invariant a level->dataflow rewrite can
// break: a producer flow that has NO relevant change in a commit must still settle and
// decrement its consumers' in_degree. Otherwise a downstream view that also depends on
// a different, changed producer would never be dispatched and would silently go stale.
//
// Shape: t1 -> view a, t2 -> view b, view c = (a append b), so c has two producers
// (in_degree 2). A commit that touches only t1 makes flow b skip (t2 unchanged) while
// flow a emits; c can only run if the skipping b still unblocked it.

use reifydb::{Database, Params, WithSubsystem, embedded};

fn setup() -> Database {
	let db = embedded::memory().with_flow(|c| c).build().unwrap();
	db
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn command(db: &Database, rql: &str) {
	db.command_as_root(rql, Params::None).unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"));
}

fn row_count(db: &Database, rql: &str) -> usize {
	let frames =
		db.command_as_root(rql, Params::None).unwrap_or_else(|e| panic!("command failed: {e:?}\nrql: {rql}"));
	frames.first().map(|f| f.row_count()).unwrap_or(0)
}

#[test]
fn skipping_producer_still_updates_shared_consumer() {
	let mut db = setup();
	admin(&db, "create namespace test");
	admin(&db, "create table test::t1 { id: int4, name: utf8 }");
	admin(&db, "create table test::t2 { id: int4, name: utf8 }");
	// Two single-source views feed one append view, so the consumer flow c has two producers.
	admin(&db, "create view test::a { id: int4, name: utf8 } as { from test::t1 }");
	admin(&db, "create view test::b { id: int4, name: utf8 } as { from test::t2 }");
	admin(&db, "create view test::c { id: int4, name: utf8 } as { from test::a append { from test::b } }");

	// Commit 1 touches both tables: flows a and b both emit, c aggregates both branches.
	command(&db, r#"INSERT test::t1 [{ id: 1, name: "Alice" }, { id: 2, name: "Bob" }]"#);
	command(&db, r#"INSERT test::t2 [{ id: 3, name: "Charlie" }]"#);
	assert_eq!(row_count(&db, "from test::c"), 3, "c must contain both branches after both tables are seeded");

	// Commit 2 touches only t1: flow a re-runs, flow b has no relevant change and skips.
	// Because c depends on both a and b, c can only re-run if the skipping b still
	// decremented c's in_degree to zero.
	command(&db, r#"UPDATE test::t1 { id, name: "Alicia" } FILTER { id == 1 }"#);

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

	db.stop().unwrap();
}
