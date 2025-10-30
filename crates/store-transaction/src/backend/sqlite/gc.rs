// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CommitVersion, EncodedKey};
use reifydb_type::Result;

use super::{SqliteBackend, write::WriteCommand};
use crate::backend::{
	diagnostic::database_error,
	gc::{BackendGarbageCollect, GcStats},
};

impl BackendGarbageCollect for SqliteBackend {
	fn compact_operator_states(&self) -> Result<GcStats> {
		let mut total_stats = GcStats::default();

		// Phase 1: Query read connection to find old versions
		let reader = self.get_reader();
		let conn = reader.lock().unwrap();

		// Find all operator_* tables
		let mut stmt = conn
			.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'operator_%'")
			.map_err(|e| reifydb_type::Error(database_error(format!("Failed to query tables: {}", e))))?;

		let mut operator_tables = Vec::new();
		let rows = stmt.query_map([], |row| row.get::<_, String>(0)).map_err(|e| {
			reifydb_type::Error(database_error(format!("Failed to fetch table names: {}", e)))
		})?;

		for row in rows {
			match row {
				Ok(name) => operator_tables.push(name),
				Err(e) => {
					return Err(reifydb_type::Error(database_error(format!(
						"Failed to read table name: {}",
						e
					))));
				}
			}
		}

		drop(stmt);

		if operator_tables.is_empty() {
			return Ok(total_stats);
		}

		println!("[GC-SQLite] Found {} operator tables", operator_tables.len());

		// Process each table
		for table_name in operator_tables {
			// First, count distinct keys in this table
			let count_query = format!("SELECT COUNT(DISTINCT key) FROM {}", table_name);
			let key_count: usize = conn.query_row(&count_query, [], |row| row.get(0)).map_err(|e| {
				reifydb_type::Error(database_error(format!("Failed to count keys: {}", e)))
			})?;

			total_stats.keys_processed += key_count;

			// Query for old versions (limit 1024 per table per cycle)
			let query = format!(
				"SELECT t1.key, t1.version
				 FROM {} t1
				 LEFT JOIN (
					 SELECT key, MAX(version) as max_version
					 FROM {}
					 GROUP BY key
				 ) t2 ON t1.key = t2.key AND t1.version = t2.max_version
				 WHERE t2.max_version IS NULL
				 LIMIT 1024",
				table_name, table_name
			);

			let mut stmt = conn.prepare(&query).map_err(|e| {
				reifydb_type::Error(database_error(format!("Failed to prepare query: {}", e)))
			})?;

			let mut deletions = Vec::new();

			let rows = stmt
				.query_map([], |row| {
					let key_blob: Vec<u8> = row.get(0)?;
					let version_i64: i64 = row.get(1)?;
					Ok((
						table_name.clone(),
						EncodedKey::new(key_blob),
						CommitVersion(version_i64 as u64),
					))
				})
				.map_err(|e| {
					reifydb_type::Error(database_error(format!(
						"Failed to query old versions: {}",
						e
					)))
				})?;

			for row in rows {
				match row {
					Ok(deletion) => {
						deletions.push(deletion);
					}
					Err(e) => {
						return Err(reifydb_type::Error(database_error(format!(
							"Failed to read row: {}",
							e
						))));
					}
				}
			}

			drop(stmt);

			if deletions.is_empty() {
				println!("[GC-SQLite] Table '{}': No old versions found", table_name);
				continue;
			}

			println!(
				"[GC-SQLite] Table '{}': Found {} old versions to delete (keys={})",
				table_name,
				deletions.len(),
				key_count
			);

			// Phase 2: Send to writer actor for deletion
			let (sender, receiver) = mpsc::channel();

			self.writer
				.send(WriteCommand::GarbageCollect {
					deletions,
					respond_to: sender,
				})
				.map_err(|_| {
					reifydb_type::Error(database_error("Failed to send GC command".to_string()))
				})?;

			// Wait for completion
			let stats = receiver.recv().map_err(|_| {
				reifydb_type::Error(database_error("Failed to receive GC response".to_string()))
			})?;

			let table_stats = stats?;
			println!(
				"[GC-SQLite] Table '{}': Deleted {} versions from {} tables",
				table_name, table_stats.versions_removed, table_stats.tables_cleaned
			);

			total_stats.merge(table_stats);
		}

		Ok(total_stats)
	}
}
