// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator_state::OperatorStateKey;
use rusqlite::Connection;

pub(crate) fn ensure_schema(conn: &Connection) {
	conn.execute_batch(
		r#"CREATE TABLE IF NOT EXISTS "operator_state" (
			"operator" INTEGER NOT NULL,
			"key" BLOB NOT NULL,
			"bytes" BLOB NOT NULL,
			PRIMARY KEY ("operator", "key")
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS "operator_seal_anchor" (
			"operator" INTEGER NOT NULL,
			"group" BLOB NOT NULL,
			"side" INTEGER NOT NULL,
			"row_number" INTEGER NOT NULL,
			"expiry" INTEGER NOT NULL,
			PRIMARY KEY ("operator", "group", "side", "row_number")
		) WITHOUT ROWID;

		CREATE INDEX IF NOT EXISTS "operator_seal_anchor_due"
			ON "operator_seal_anchor" ("operator", "group", "expiry");

		CREATE TABLE IF NOT EXISTS "flow_checkpoint" (
			"flow" INTEGER NOT NULL PRIMARY KEY,
			"version" INTEGER NOT NULL
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS "operator_state_census" (
			"operator" INTEGER NOT NULL,
			"keyspace" BLOB NOT NULL,
			"keys" INTEGER NOT NULL,
			"key_bytes" INTEGER NOT NULL,
			"value_bytes" INTEGER NOT NULL,
			PRIMARY KEY ("operator", "keyspace")
		) WITHOUT ROWID;

		DROP TRIGGER IF EXISTS "operator_state_census_insert";
		DROP TRIGGER IF EXISTS "operator_state_census_update";
		DROP TRIGGER IF EXISTS "operator_state_census_delete";"#,
	)
	.expect("operator state schema could not be created");

	seed_census(conn);
}

fn seed_census(conn: &Connection) {
	let seeded: i64 = conn
		.query_row(r#"SELECT COUNT(*) FROM "operator_state_census""#, [], |row| row.get(0))
		.expect("operator state census count failed");
	if seeded > 0 {
		return;
	}
	let keyspace = OperatorStateKey::KEYSPACE_INNER_OFFSET + 1;
	conn.execute(
		&format!(r#"INSERT INTO "operator_state_census"
				("operator", "keyspace", "keys", "key_bytes", "value_bytes")
			   SELECT "operator", substr("key", {keyspace}, 1), COUNT(*),
			          SUM(LENGTH("key")), SUM(LENGTH("bytes"))
			   FROM "operator_state"
			   GROUP BY "operator", substr("key", {keyspace}, 1)"#),
		[],
	)
	.expect("operator state census could not be seeded");
}
