// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression: compile_join_subquery in crates/rql/src/plan/logical/query/join.rs
// previously called nodes.first() and discarded all pipeline operators after the
// from clause. The right side of a streaming join is the reference (populated
// first); the left side is the driver (triggers the join).

use reifydb::{Database, Params, WithSubsystem, embedded};

fn setup() -> Database {
	let mut db = embedded::memory().with_flow(|c| c).build().unwrap();
	db.start().unwrap();
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

// Insert 3 price rows for the same mint, then 1 swap. Without the fix the
// pipeline operators (sort, map, distinct) are silently dropped, so the join
// fans out against all 3 price rows and emits 3 rows. With the fix, distinct
// collapses them to 1 and the join emits exactly 1 row.
#[test]
fn distinct_in_join_subquery_deduplicates() {
	let mut db = setup();
	admin(&db, "create namespace test");
	admin(&db, "create table test::prices { mint: utf8, slot: uint8, price: float8 }");
	admin(&db, "create table test::swaps { swap_id: uint8, quote_mint: utf8 }");
	admin(
		&db,
		"create view test::result \
         { swap_id: uint8, quote_mint: utf8, p_mint: utf8, p_price: float8 } as { \
             from test::swaps \
             inner join { from test::prices | sort { slot: desc } | map { mint, price } | distinct { mint } } as p \
             using (quote_mint, p.mint) \
         }",
	);

	// Right side first: 3 price rows for the same mint at different slots.
	command(
		&db,
		r#"INSERT test::prices [
            { mint: "USDC", slot: 3, price: 1.0 },
            { mint: "USDC", slot: 2, price: 1.0 },
            { mint: "USDC", slot: 1, price: 1.0 }
        ]"#,
	);
	// Left side second: 1 swap triggers the join.
	command(&db, r#"INSERT test::swaps [{ swap_id: 1, quote_mint: "USDC" }]"#);

	assert_eq!(
		row_count(&db, "from test::result"),
		1,
		"distinct should collapse 3 USDC price rows to 1 before the join"
	);

	db.stop().unwrap();
}

// Verify that a map operator inside the join subquery is compiled and runs.
// The join still produces 1 output row, confirming the pipeline executed.
#[test]
fn map_in_join_subquery_executes() {
	let mut db = setup();
	admin(&db, "create namespace test2");
	admin(&db, "create table test2::prices { mint: utf8, slot: uint8, price: float8 }");
	admin(&db, "create table test2::swaps { swap_id: uint8, quote_mint: utf8 }");
	admin(
		&db,
		"create view test2::result \
         { swap_id: uint8, quote_mint: utf8, p_mint: utf8 } as { \
             from test2::swaps \
             inner join { from test2::prices | map { mint } } as p \
             using (quote_mint, p.mint) \
         }",
	);

	// Right side first.
	command(&db, r#"INSERT test2::prices [{ mint: "USDC", slot: 1, price: 1.0 }]"#);
	// Left side second: triggers the join.
	command(&db, r#"INSERT test2::swaps [{ swap_id: 1, quote_mint: "USDC" }]"#);

	assert_eq!(
		row_count(&db, "from test2::result"),
		1,
		"join with map in the subquery pipeline should produce 1 matched row"
	);

	db.stop().unwrap();
}

// Ensure a plain single-node subquery (no pipeline) still works after the
// refactor. Guards against regressions in the simple case.
#[test]
fn plain_join_subquery_without_pipeline_unchanged() {
	let mut db = setup();
	admin(&db, "create namespace test3");
	admin(&db, "create table test3::a { id: uint8, val: utf8 }");
	admin(&db, "create table test3::b { id: uint8, name: utf8 }");
	admin(
		&db,
		"create view test3::result \
         { id: uint8, val: utf8, b_id: uint8, b_name: utf8 } as { \
             from test3::a \
             inner join { from test3::b } as b \
             using (id, b.id) \
         }",
	);

	// Right side first.
	command(&db, r#"INSERT test3::b [{ id: 1, name: "y" }]"#);
	// Left side second: triggers the join.
	command(&db, r#"INSERT test3::a [{ id: 1, val: "x" }]"#);

	assert_eq!(
		row_count(&db, "from test3::result"),
		1,
		"single-node join subquery must still produce exactly 1 matched row"
	);

	db.stop().unwrap();
}
