// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{age_past_ttl, await_drained, one_row_per_tick, ttl_db};

fn rows(target: &str, count: usize) -> String {
	let values: Vec<String> = (0..count).map(|n| format!("{{ n: {n} }}")).collect();
	format!("insert {target} [{}]", values.join(", "))
}

#[test]
fn every_table_drains_when_one_tick_reaches_only_one_of_them() {
	// A tick that always restarts at the first object never reaches the last, and it expires forever.
	let path = TempDbPath::new("ttl_multi_object_table");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	for name in ["a", "b", "c"] {
		db.admin(&format!(
			"create table test::{name} {{ n: int4 }} with {{ time: processing, row: {{ ttl: 1s }} }}"
		));
		db.command(&rows(&format!("test::{name}"), 3));
	}

	age_past_ttl();
	for name in ["a", "b", "c"] {
		await_drained(&db, &format!("from test::{name}"), 0);
	}
	db.stop();
}

#[test]
fn a_small_table_is_not_starved_by_a_large_one() {
	// A budget spent entirely on the backlog ahead of it leaves the small object unvisited forever.
	let path = TempDbPath::new("ttl_multi_object_table_starve");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin("create table test::big { n: int4 } with { time: processing, row: { ttl: 1s } }");
	db.admin("create table test::small { n: int4 } with { time: processing, row: { ttl: 1s } }");
	db.command(&rows("test::big", 20));
	db.command(&rows("test::small", 2));

	age_past_ttl();
	await_drained(&db, "from test::small", 0);
	await_drained(&db, "from test::big", 0);
	db.stop();
}

#[test]
fn a_table_without_a_ttl_is_never_touched() {
	// Only objects that declare a ttl may be scanned, or an untimed table quietly loses rows.
	let path = TempDbPath::new("ttl_multi_object_table_untimed");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin("create table test::timed { n: int4 } with { time: processing, row: { ttl: 1s } }");
	db.admin("create table test::untimed { n: int4 }");
	db.command(&rows("test::timed", 3));
	db.command(&rows("test::untimed", 3));

	age_past_ttl();
	await_drained(&db, "from test::timed", 0);
	assert_eq!(db.row_count("from test::untimed"), 3, "a table with no ttl must keep every row");
	db.stop();
}
