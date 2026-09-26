// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rusqlite::Connection;

use crate::persistent::sqlite::sql::CHECKPOINT_SCHEMA_SQL;

pub(crate) fn ensure_schema(conn: &Connection) {
	conn.execute_batch(CHECKPOINT_SCHEMA_SQL).expect("operator state schema could not be created");
}
