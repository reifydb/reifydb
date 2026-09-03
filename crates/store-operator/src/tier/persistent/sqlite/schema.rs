// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::keyspace::KEYSPACES;
use rusqlite::Connection;

use crate::tier::persistent::sqlite::typed::create_table;

pub(crate) fn ensure_schema(conn: &Connection) {
	conn.execute_batch(
		r#"CREATE TABLE IF NOT EXISTS "flow_checkpoint" (
			"flow" INTEGER NOT NULL PRIMARY KEY,
			"version" INTEGER NOT NULL
		) WITHOUT ROWID;

		DROP TRIGGER IF EXISTS "operator_state_census_insert";
		DROP TRIGGER IF EXISTS "operator_state_census_update";
		DROP TRIGGER IF EXISTS "operator_state_census_delete";
		DROP TABLE IF EXISTS "operator_state_census";
		DROP TABLE IF EXISTS "operator_state";
		DROP INDEX IF EXISTS "operator_join_expiry_due";
		DROP TABLE IF EXISTS "operator_join_expiry";"#,
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
