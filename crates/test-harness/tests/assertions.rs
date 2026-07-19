// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg(feature = "database")]

use reifydb_test_harness::{
	assert::{FrameAssert, assert_frames_eq},
	db::TestDb,
};

fn seeded() -> TestDb {
	let db = TestDb::memory();
	db.admin("create namespace test");
	db.admin("create table test::items { id: int4 }");
	db.command("insert test::items [{ id: 1 }, { id: 2 }, { id: 3 }]");
	db
}

#[test]
fn row_count_matches_actual() {
	seeded().query("from test::items").assert().row_count(3);
}

#[test]
#[should_panic(expected = "expected 9 rows")]
fn row_count_mismatch_panics() {
	// A wrong expectation must fail loudly; a silently-passing assertion is worse than no
	// assertion at all.
	seeded().query("from test::items").assert().row_count(9);
}

#[test]
#[should_panic(expected = "frame has no column")]
fn unknown_column_panics() {
	seeded().query("from test::items").assert().column("nonexistent", &[]);
}

#[test]
#[should_panic(expected = "expected 0 rows")]
fn is_empty_on_non_empty_panics() {
	seeded().query("from test::items").assert().is_empty();
}

#[test]
fn identical_frames_are_equal() {
	let db = seeded();
	let a = db.query("from test::items");
	let b = db.query("from test::items");
	assert_frames_eq(&a, &b);
}

#[test]
#[should_panic(expected = "mismatch")]
fn different_frames_are_not_equal() {
	let db = seeded();
	let all = db.query("from test::items");
	let filtered = db.query("from test::items filter { id > 1 }");
	assert_frames_eq(&all, &filtered);
}
