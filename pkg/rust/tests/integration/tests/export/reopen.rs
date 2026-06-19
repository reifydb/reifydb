// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Regression: when a persistent (sqlite) database is reopened, the catalog cache must repopulate
// each shape's columns. Ring buffers previously cached empty columns on reopen, so `from ns::rb`
// failed with ENG_001 "mismatched column count: expected 0, got 2". All shape kinds must scan
// correctly after a reopen, returning the same rows as before the reopen.

use std::{
	fs,
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb::{Database, Params, SqliteConfig, Value, embedded};

fn unique_db_path(tag: &str) -> std::path::PathBuf {
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	std::env::temp_dir().join(format!("reifydb_reopen_{tag}_{}_{}.reifydb", std::process::id(), nanos))
}

fn rows(db: &Database, query: &str) -> Vec<Vec<(String, Value)>> {
	db.query_as_root(query, Params::None)
		.expect("query failed")
		.into_iter()
		.flat_map(|frame| frame.to_rows())
		.collect()
}

#[test]
fn ringbuffer_scans_after_sqlite_reopen() {
	let path = unique_db_path("rb");
	let _ = fs::remove_file(&path);

	let before = {
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
		db.admin_as_root(
			"create namespace p; create ringbuffer p::rb { id: int4, msg: utf8 } with { capacity: 3 };",
			Params::None,
		)
		.unwrap();
		db.command_as_root(
			"insert p::rb [{ id: 1, msg: 'a' }, { id: 2, msg: 'b' }, { id: 3, msg: 'c' }];",
			Params::None,
		)
		.unwrap();
		let before = rows(&db, "from p::rb");
		db.stop().unwrap();
		before
	};
	assert_eq!(before.len(), 3, "ring buffer should have 3 rows before reopen");

	let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
	let after = rows(&db, "from p::rb");
	db.stop().unwrap();

	assert_eq!(before, after, "ring buffer rows must be identical after reopen");
	let _ = fs::remove_file(&path);
}

#[test]
fn series_scans_after_sqlite_reopen() {
	let path = unique_db_path("series");
	let _ = fs::remove_file(&path);

	let before = {
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
		db.admin_as_root(
			"create namespace p; create series p::s { ts: datetime, v: int4 } with { key: ts, precision: millisecond };",
			Params::None,
		)
		.unwrap();
		db.command_as_root(
			"insert p::s [{ ts: @2024-01-01T00:00:00Z, v: 1 }, { ts: @2024-01-01T00:00:01Z, v: 2 }];",
			Params::None,
		)
		.unwrap();
		let before = rows(&db, "from p::s");
		db.stop().unwrap();
		before
	};
	assert_eq!(before.len(), 2, "series should have 2 rows before reopen");

	let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
	let after = rows(&db, "from p::s");
	db.stop().unwrap();

	assert_eq!(before, after, "series rows must be identical after reopen");
	let _ = fs::remove_file(&path);
}
