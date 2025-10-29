// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{CommitVersion, EncodedKey, Result};
use rusqlite::params;

use super::{as_flow_node_state_key, operator_name, source_name};
use crate::backend::{multi::BackendMultiVersionContains, sqlite::SqliteBackend};

/// Helper function to get the appropriate table name for a given key
fn get_table_name(key: &EncodedKey) -> Result<&'static str> {
	// Check if it's a FlowNodeStateKey first
	if as_flow_node_state_key(key).is_some() {
		operator_name(key)
	} else {
		// Use source_name for everything else (RowKey or multi)
		source_name(key)
	}
}

impl BackendMultiVersionContains for SqliteBackend {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		let reader = self.get_reader();
		let guard = reader.lock().unwrap();

		let table = get_table_name(key)?;
		let query = format!("SELECT EXISTS(SELECT 1 FROM {} WHERE key = ? AND version <= ?)", table);

		Ok(guard.query_row(&query, params![key.to_vec(), version.0], |row| row.get(0)).unwrap())
	}
}
