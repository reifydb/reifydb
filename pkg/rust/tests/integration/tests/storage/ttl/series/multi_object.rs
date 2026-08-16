// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TempDbPath;

use crate::storage::ttl::{age_past_ttl, await_drained, one_row_per_tick, ttl_db};

fn ddl(name: &str, ttl: Option<&str>) -> String {
	match ttl {
		Some(ttl) => format!(
			"create series test::{name} {{ ts: int8, n: int4 }} with {{ time: processing, key: ts, row: {{ ttl: {ttl} }} }}"
		),
		None => format!("create series test::{name} {{ ts: int8, n: int4 }} with {{ key: ts }}"),
	}
}

fn rows(target: &str, count: usize) -> String {
	let values: Vec<String> = (0..count).map(|n| format!("{{ ts: {n}, n: {n} }}")).collect();
	format!("insert {target} [{}]", values.join(", "))
}

#[test]
fn every_series_drains_when_one_tick_reaches_only_one_of_them() {
	// A tick that always restarts at the first object never reaches the last, and it expires forever.
	let path = TempDbPath::new("ttl_multi_object_series");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	for name in ["a", "b", "c"] {
		db.admin(&ddl(name, Some("1s")));
		db.command(&rows(&format!("test::{name}"), 3));
	}

	age_past_ttl();
	for name in ["a", "b", "c"] {
		await_drained(&db, &format!("from test::{name}"), 0);
	}
	db.stop();
}

#[test]
fn a_small_series_is_not_starved_by_a_large_one() {
	// A budget spent entirely on the backlog ahead of it leaves the small object unvisited forever.
	let path = TempDbPath::new("ttl_multi_object_series_starve");

	let mut db = ttl_db(&path, one_row_per_tick());
	db.admin("create namespace test");
	db.admin(&ddl("big", Some("1s")));
	db.admin(&ddl("small", Some("1s")));
	db.command(&rows("test::big", 20));
	db.command(&rows("test::small", 2));

	age_past_ttl();
	await_drained(&db, "from test::small", 0);
	await_drained(&db, "from test::big", 0);
	db.stop();
}

#[test]
fn a_series_without_a_ttl_is_never_touched() {
	// Only objects that declare a ttl may be scanned, or an untimed series quietly loses rows.
	let path = TempDbPath::new("ttl_multi_object_series_untimed");

	let mut db = ttl_db(&path, []);
	db.admin("create namespace test");
	db.admin(&ddl("timed", Some("1s")));
	db.admin(&ddl("untimed", None));
	db.command(&rows("test::timed", 3));
	db.command(&rows("test::untimed", 3));

	age_past_ttl();
	await_drained(&db, "from test::timed", 0);
	assert_eq!(db.row_count("from test::untimed"), 3, "a series with no ttl must keep every row");
	db.stop();
}
