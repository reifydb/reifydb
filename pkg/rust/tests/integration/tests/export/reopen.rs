// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Regression: when a persistent (sqlite) database is reopened, the catalog cache must repopulate
// each shape's columns. Ring buffers previously cached empty columns on reopen, so `from ns::rb`
// failed with ENG_001 "mismatched column count: expected 0, got 2". All shape kinds must scan
// correctly after a reopen, returning the same rows as before the reopen.

use reifydb_test_harness::{
	assert::rows,
	db::{TempDbPath, TestDb},
};

#[test]
fn ringbuffer_scans_after_sqlite_reopen() {
	let path = TempDbPath::new("reopen_rb");

	let before = {
		let mut db = TestDb::sqlite_at(&path);
		db.admin("create namespace p; create ringbuffer p::rb { id: int4, msg: utf8 } with { capacity: 3 };");
		db.command("insert p::rb [{ id: 1, msg: 'a' }, { id: 2, msg: 'b' }, { id: 3, msg: 'c' }];");
		let before = rows(&db.query("from p::rb"));
		db.stop();
		before
	};
	assert_eq!(before.len(), 3, "ring buffer should have 3 rows before reopen");

	let mut db = TestDb::sqlite_at(&path);
	let after = rows(&db.query("from p::rb"));
	db.stop();

	assert_eq!(before, after, "ring buffer rows must be identical after reopen");
}

#[test]
fn series_scans_after_sqlite_reopen() {
	let path = TempDbPath::new("reopen_series");

	let before = {
		let mut db = TestDb::sqlite_at(&path);
		db.admin(
			"create namespace p; create series p::s { ts: datetime, v: int4 } with { key: ts, precision: millisecond };",
		);
		db.command("insert p::s [{ ts: @2024-01-01T00:00:00Z, v: 1 }, { ts: @2024-01-01T00:00:01Z, v: 2 }];");
		let before = rows(&db.query("from p::s"));
		db.stop();
		before
	};
	assert_eq!(before.len(), 2, "series should have 2 rows before reopen");

	let mut db = TestDb::sqlite_at(&path);
	let after = rows(&db.query("from p::s"));
	db.stop();

	assert_eq!(before, after, "series rows must be identical after reopen");
}
