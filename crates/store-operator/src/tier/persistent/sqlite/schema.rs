// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::keyspace::KEYSPACES;
use rusqlite::Connection;

use crate::tier::persistent::sqlite::typed::create_table;

pub(crate) fn ensure_schema(conn: &Connection) {
	conn.execute_batch(
		r#"CREATE TABLE IF NOT EXISTS "operator_join_expiry" (
			"operator" INTEGER NOT NULL,
			"group" BLOB NOT NULL,
			"side" INTEGER NOT NULL,
			"row_number" INTEGER NOT NULL,
			"at" INTEGER NOT NULL,
			PRIMARY KEY ("operator", "group", "side", "row_number")
		) WITHOUT ROWID;

		CREATE INDEX IF NOT EXISTS "operator_join_expiry_due"
			ON "operator_join_expiry" ("operator", "group", "at");

		CREATE TABLE IF NOT EXISTS "flow_checkpoint" (
			"flow" INTEGER NOT NULL PRIMARY KEY,
			"version" INTEGER NOT NULL
		) WITHOUT ROWID;

		DROP TRIGGER IF EXISTS "operator_state_census_insert";
		DROP TRIGGER IF EXISTS "operator_state_census_update";
		DROP TRIGGER IF EXISTS "operator_state_census_delete";
		DROP TABLE IF EXISTS "operator_state_census";
		DROP TABLE IF EXISTS "operator_state";"#,
	)
	.expect("operator state schema could not be created");

	ensure_keyspace_tables(conn);
}

fn ensure_keyspace_tables(conn: &Connection) {
	let mut batch = String::new();
	for spec in KEYSPACES {
		batch.push_str(&create_table(spec));
		batch.push('\n');
	}
	conn.execute_batch(&batch).expect("operator keyspace tables could not be created");
}
