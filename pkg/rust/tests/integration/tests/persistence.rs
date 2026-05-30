// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	fs,
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb::{Params, SqliteConfig, embedded};
use reifydb_value::value::frame::frame::Frame;

fn unique_db_path(tag: &str) -> std::path::PathBuf {
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	std::env::temp_dir().join(format!("reifydb_persist_{tag}_{}_{}.reifydb", std::process::id(), nanos))
}

fn row_count(frames: &[Frame]) -> usize {
	frames.iter().map(|f| f.columns.first().map_or(0, |c| c.data.len())).sum()
}

#[test]
fn persistent_false_rejected_when_store_has_no_buffer() {
	let (config, _guard) = SqliteConfig::in_memory();
	let mut db = embedded::sqlite_without_buffer(config).build().unwrap();

	db.admin_as_root("create namespace demo", Params::None).unwrap();

	let result = db.admin_as_root(
		"create table demo::t { id: uint8 } with { row: { ttl: { duration: '1m', on: created, mode: drop }, persistent: false } }",
		Params::None,
	);
	assert!(result.is_err(), "persistent: false must be rejected when the store has no buffer tier");
	assert_eq!(result.unwrap_err().diagnostic().code, "CA_086");

	// a default (persistent) table is still fine in an unbuffered store
	db.admin_as_root("create table demo::keep { id: uint8 }", Params::None).unwrap();

	db.stop().unwrap();
}

#[test]
fn persistent_false_rows_are_not_durable_after_reopen() {
	let path = unique_db_path("durable");
	let _ = fs::remove_file(&path);

	{
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();

		db.admin_as_root("create namespace demo", Params::None).unwrap();
		db.admin_as_root("create table demo::keep { id: uint8 }", Params::None).unwrap();
		db.admin_as_root(
			"create table demo::transient { id: uint8 } with { row: { ttl: { duration: '1h', on: created, mode: drop }, persistent: false } }",
			Params::None,
		)
		.unwrap();

		db.command_as_root("insert demo::keep [{ id: 1 }, { id: 2 }]", Params::None).unwrap();
		db.command_as_root("insert demo::transient [{ id: 1 }, { id: 2 }, { id: 3 }]", Params::None).unwrap();

		// while the process is alive the transient rows are queryable from the in-memory buffer
		assert_eq!(
			row_count(&db.query_as_root("from demo::transient", Params::None).unwrap()),
			3,
			"transient rows should flow through the buffer tier within the process"
		);

		// stop() flushes the buffer into sqlite; the transient shape is filtered out of that flush
		db.stop().unwrap();
	}

	{
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();

		assert_eq!(
			row_count(&db.query_as_root("from demo::keep", Params::None).unwrap()),
			2,
			"persistent table rows survive reopen"
		);
		assert_eq!(
			row_count(&db.query_as_root("from demo::transient", Params::None).unwrap()),
			0,
			"persistent: false rows were never written to sqlite, so they are gone after reopen"
		);

		db.stop().unwrap();
	}

	let _ = fs::remove_file(&path);
}
