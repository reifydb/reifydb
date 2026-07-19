// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::{
	assert::{FrameAssert, ResultAssert},
	db::{TempDbPath, TestDb},
};

#[test]
fn persistent_false_rejected_when_store_has_no_buffer() {
	let mut db = TestDb::sqlite_without_buffer_memory();

	db.admin("create namespace demo");

	db.try_admin(
		"create table demo::t { id: uint8 } with { row: { ttl: { duration: '1m', mode: drop }, persistent: false } }",
	)
	.assert_error("CA_086");

	// a default (persistent) table is still fine in an unbuffered store
	db.admin("create table demo::keep { id: uint8 }");

	db.stop();
}

#[test]
fn persistent_false_rows_are_not_durable_after_reopen() {
	let path = TempDbPath::new("persist_durable");

	{
		let mut db = TestDb::sqlite_at(&path);

		db.admin("create namespace demo");
		db.admin("create table demo::keep { id: uint8 }");
		db.admin(
			"create table demo::transient { id: uint8 } with { row: { ttl: { duration: '1h', mode: drop }, persistent: false } }",
		);

		db.command("insert demo::keep [{ id: 1 }, { id: 2 }]");
		db.command("insert demo::transient [{ id: 1 }, { id: 2 }, { id: 3 }]");

		// while the process is alive the transient rows are queryable from the in-memory buffer
		db.query("from demo::transient").assert().row_count(3);

		// stop() flushes the buffer into sqlite; the transient shape is filtered out of that flush
		db.stop();
	}

	{
		let mut db = TestDb::sqlite_at(&path);

		// persistent table rows survive reopen
		db.query("from demo::keep").assert().row_count(2);
		// persistent: false rows were never written to sqlite, so they are gone after reopen
		db.query("from demo::transient").assert().is_empty();

		db.stop();
	}
}
