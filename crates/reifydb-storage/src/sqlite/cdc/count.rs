// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::interface::CdcCount;
use reifydb_core::{Result, Version};
use rusqlite::params;

impl CdcCount for Sqlite {
    fn count(&self, version: Version) -> Result<usize> {
        let conn = self.get_conn();

        let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM cdc WHERE version = ?").unwrap();

        let count: usize = stmt.query_row(params![version as i64], |row| row.get(0)).unwrap();

        Ok(count)
    }
}