// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::interface::VersionedContains;
use reifydb_core::{EncodedKey, Version};
use rusqlite::params;

impl VersionedContains for Sqlite {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool {
        let conn = self.get_conn();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM versioned WHERE key = ? AND version <= ?)",
                params![key.to_vec(), version],
                |row| row.get(0),
            )
            .unwrap();
        exists
    }
}