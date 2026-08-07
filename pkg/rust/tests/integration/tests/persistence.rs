// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::{TempDbPath, TestDb};
use reifydb_test_harness::assert::FrameAssert;

#[test]
fn persistent_false_rows_are_not_durable_after_reopen() {
	let path = TempDbPath::new("persist_durable");

	{
		let mut db = TestDb::sqlite_at(&path);

		db.admin("create namespace demo");
		db.admin("create table demo::keep { id: uint8 }");
		db.admin(
			"create table demo::transient { id: uint8 } with { time: processing, row: { ttl: { duration: '1h', announce: false }, persistent: false } }",
		);

		db.command("insert demo::keep [{ id: 1 }, { id: 2 }]");
		db.command("insert demo::transient [{ id: 1 }, { id: 2 }, { id: 3 }]");

		// while the process is alive the transient rows are queryable from the in-memory buffer
		db.query("from demo::transient").assert().row_count(3);

		// stop() flushes the buffer into sqlite; the transient object is filtered out of that flush
		db.stop();
	}

	{
		let mut db = TestDb::sqlite_at(&path);

		db.query("from demo::keep").assert().row_count(2);
		// persistent: false rows were never written to sqlite, so they are gone after reopen
		db.query("from demo::transient").assert().is_empty();

		db.stop();
	}
}
