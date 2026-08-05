// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|c| c).build().unwrap())
}

fn settled_row_count(db: &TestDb, rql: &str) -> usize {
	// The views are deferred, so the count is only meaningful once every flow has consumed the
	// inserts; waiting for a target count instead could pass on a transient state.
	assert!(db.await_all_flows(StdDuration::from_secs(10)), "flows must catch up before asserting");
	db.row_count(rql)
}

#[test]
fn distinct_in_join_subquery_deduplicates() {
	// Drop the subquery's pipeline operators and the join fans out over all 3 price rows;
	// distinct has to collapse them so exactly 1 row is emitted.
	let mut db = setup();
	db.admin("create namespace test");
	db.admin("create table test::prices { mint: utf8, slot: uint8, price: float8 }");
	db.admin("create table test::swaps { swap_id: uint8, quote_mint: utf8 }");
	db.admin("create view test::result \
         { swap_id: uint8, quote_mint: utf8, p_mint: utf8, p_price: float8 } as { \
             from test::swaps \
             inner join { from test::prices | map { mint, price } | distinct { mint } } as p \
             using (quote_mint, p.mint) \
         }");

	// The right side is the reference and must land first; the left side drives the join.
	db.command(
		r#"INSERT test::prices [
            { mint: "USDC", slot: 2, price: 1.0 },
            { mint: "USDC", slot: 1, price: 1.0 }
            { mint: "USDC", slot: 3, price: 1.0 },
        ]"#,
	);
	// Left side second: 1 swap triggers the join.
	db.command(r#"INSERT test::swaps [{ swap_id: 1, quote_mint: "USDC" }]"#);

	assert_eq!(
		settled_row_count(&db, "from test::result"),
		1,
		"distinct should collapse 3 USDC price rows to 1 before the join"
	);

	db.stop();
}

#[test]
fn map_in_join_subquery_executes() {
	// A map inside the join subquery must be compiled and run, not dropped.
	let mut db = setup();
	db.admin("create namespace test2");
	db.admin("create table test2::prices { mint: utf8, slot: uint8, price: float8 }");
	db.admin("create table test2::swaps { swap_id: uint8, quote_mint: utf8 }");
	db.admin("create view test2::result \
         { swap_id: uint8, quote_mint: utf8, p_mint: utf8 } as { \
             from test2::swaps \
             inner join { from test2::prices | map { mint } } as p \
             using (quote_mint, p.mint) \
         }");

	// Right side first.
	db.command(r#"INSERT test2::prices [{ mint: "USDC", slot: 1, price: 1.0 }]"#);
	// Left side second: triggers the join.
	db.command(r#"INSERT test2::swaps [{ swap_id: 1, quote_mint: "USDC" }]"#);

	assert_eq!(
		settled_row_count(&db, "from test2::result"),
		1,
		"join with map in the subquery pipeline should produce 1 matched row"
	);

	db.stop();
}

#[test]
fn plain_join_subquery_without_pipeline_unchanged() {
	// The pipeline-free subquery form must keep working alongside the pipelined one.
	let mut db = setup();
	db.admin("create namespace test3");
	db.admin("create table test3::a { id: uint8, val: utf8 }");
	db.admin("create table test3::b { id: uint8, name: utf8 }");
	db.admin("create view test3::result \
         { id: uint8, val: utf8, b_id: uint8, b_name: utf8 } as { \
             from test3::a \
             inner join { from test3::b } as b \
             using (id, b.id) \
         }");

	// Right side first.
	db.command(r#"INSERT test3::b [{ id: 1, name: "y" }]"#);
	// Left side second: triggers the join.
	db.command(r#"INSERT test3::a [{ id: 1, val: "x" }]"#);

	assert_eq!(
		settled_row_count(&db, "from test3::result"),
		1,
		"single-node join subquery must still produce exactly 1 matched row"
	);

	db.stop();
}
