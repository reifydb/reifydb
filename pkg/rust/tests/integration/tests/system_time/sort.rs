// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `#time` is a substrate-owned per-row stamp carried beside the rows rather than inside them, so
//! every node that permutes or truncates rows has to move it in lockstep with the other three system
//! vectors. Nothing in the type system ties them together: a node that reorders `row_numbers`,
//! `created_at` and `updated_at` and forgets `time` still compiles and still returns the right
//! values for every ordinary column, which is why this went unnoticed. The two ways it breaks have
//! very different signatures - a length that no longer matches the row count, and a length that
//! matches while every entry belongs to a different row - and only the first is loud, so both are
//! pinned here.
//!
//! An event-time table is what makes the silent case observable at all: with `ts` declared, `#time`
//! is populated from a column that is itself queryable, so the stamp can be checked against the row
//! it is supposed to belong to instead of merely being counted.

use reifydb::{RuntimeConfig, embedded as db_embedded};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::Value;

fn seeded_db() -> TestDb {
	let db = TestDb::from(
		db_embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"),
	);
	db.admin("CREATE NAMESPACE st");
	db.admin("CREATE TABLE st::t { id: int4, at: datetime } with { ts: at }");
	// Inserted in neither id nor at order, so every ordering below is a real permutation. Rows that
	// arrive already sorted would let an unpermuted #time vector pass by coincidence.
	db.command(
		r#"INSERT st::t [
			{ id: 5, at: "2026-01-01T00:00:05Z" },
			{ id: 3, at: "2026-01-01T00:00:03Z" },
			{ id: 1, at: "2026-01-01T00:00:01Z" },
			{ id: 4, at: "2026-01-01T00:00:04Z" },
			{ id: 2, at: "2026-01-01T00:00:02Z" }
		]"#,
	);
	db
}

/// `#time` is populated from `at`, so the two must agree row for row no matter how the rows were
/// reordered or trimmed on the way out.
fn assert_time_tracks_its_row(db: &TestDb, rql: &str, expected_rows: usize) {
	let frames = db.query(rql);
	let frame = frames.first().unwrap_or_else(|| panic!("{rql}: no frame"));
	let at = frame.columns.iter().find(|c| c.name == "at").unwrap_or_else(|| panic!("{rql}: no `at` column"));

	assert_eq!(at.data.len(), expected_rows, "{rql}: unexpected row count");
	assert_eq!(
		frame.time().len(),
		expected_rows,
		"{rql}: #time holds {} stamps for {expected_rows} rows - it was not trimmed with the rows",
		frame.time().len()
	);
	for i in 0..expected_rows {
		assert_eq!(
			Value::DateTime(frame.time()[i]),
			at.data.get_value(i),
			"{rql}: #time[{i}] carries another row's stamp - it was not permuted with the rows",
		);
	}
}

#[test]
// Intent: a sort with no limit permutes in place. Mutation: drop the `time` reorder in
// SortNode::apply and #time keeps input order while every column moves, so row 0 reports the
// stamp of whichever row happened to be scanned first.
fn a_sort_carries_time_with_its_row() {
	let db = seeded_db();
	assert_time_tracks_its_row(&db, "FROM st::t | SORT { at: DESC }", 5);
	assert_time_tracks_its_row(&db, "FROM st::t | SORT { id: ASC }", 5);
}

#[test]
// Intent: sort followed by take is the top-k path, which selects a subset by heap and then permutes
// it - so it both trims and reorders. Mutation: drop the `time` reorder there and the vector keeps
// all five stamps for a three row frame, which is the length mismatch the multi-frame codec test
// surfaces as a decode failure rather than as anything legible.
fn a_sorted_take_trims_and_permutes_time_with_its_rows() {
	let db = seeded_db();
	assert_time_tracks_its_row(&db, "FROM st::t | SORT { at: DESC } | TAKE 3", 3);
	assert_time_tracks_its_row(&db, "FROM st::t | SORT { id: ASC } | TAKE 2", 2);
	// Taking everything routes through top-k's other arm (row_count <= limit), which sorts in
	// place instead of going through the heap and is a separate copy of the same reorder.
	assert_time_tracks_its_row(&db, "FROM st::t | SORT { at: ASC } | TAKE 5", 5);
}
