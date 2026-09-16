// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Frame, Value,
	testing::db::{TempDbPath, TestDb},
};
use reifydb_test_harness::assert::column_values;

const AGGREGATE: &str = "from test::src | aggregate { d: stats::digest(v, 0.01) } by { k }";

const PERCENTILES: &str = "from test::t | sort { k } | map { k, d, p: stats::approx_percentile(d, 0.99) }";

fn populate(db: &TestDb) {
	db.admin("create namespace test");
	db.admin("create table test::src { k: int4, v: float8 }");
	let rows: Vec<String> = (0..400)
		.map(|i| {
			let magnitude = 1.02f64.powi(i % 250);
			let v = if i % 5 == 0 {
				-magnitude
			} else {
				magnitude
			};
			format!("{{ k: {}, v: {v} }}", i % 3)
		})
		.collect();
	db.command(&format!("insert test::src [{}]", rows.join(", ")));
	db.admin("create table test::t { k: int4, d: digest(float8, 0.01) }");
	db.command(&format!("let $agg = {AGGREGATE}; insert test::t $agg"));
}

fn columns(frames: &[Frame]) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	(column_values(&frames[0], "k"), column_values(&frames[0], "d"), column_values(&frames[0], "p"))
}

#[test]
fn a_stored_digest_gives_the_same_percentile_after_a_reopen() {
	// A digest that did not reach the persistent tier byte for byte would move p99 after a restart.
	let path = TempDbPath::new("digest_reopen");

	let before = {
		let mut db = TestDb::sqlite_at(&path);
		populate(&db);
		let before = columns(&db.query(PERCENTILES));
		assert_eq!(before.0.len(), 3, "precondition: one stored digest per key");
		assert!(
			before.2.iter().all(|p| matches!(p, Value::Float8(_))),
			"precondition: every stored digest yields a percentile, got {:?}",
			before.2
		);
		db.stop();
		before
	};

	let mut db = TestDb::sqlite_at(&path);
	let after = columns(&db.query(PERCENTILES));
	assert_eq!(after, before);
	db.stop();
}
