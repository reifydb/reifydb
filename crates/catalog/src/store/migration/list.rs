// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::migration::{Migration, MigrationEvent},
	key::{migration::MigrationKey, migration_event::MigrationEventKey},
};
use reifydb_transaction::transaction::Transaction;

use super::{migration_event_from_row, migration_from_row};
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_migrations(txn: &mut Transaction<'_>) -> Result<Vec<Migration>> {
		let range = MigrationKey::full_scan();
		let mut results = Vec::new();
		for entry in txn.range(range, 1024)? {
			let entry = entry?;
			results.push(migration_from_row(&entry.row));
		}
		Ok(results)
	}

	pub(crate) fn list_migration_events(txn: &mut Transaction<'_>) -> Result<Vec<MigrationEvent>> {
		let range = MigrationEventKey::full_scan();
		let mut results = Vec::new();
		for entry in txn.range(range, 1024)? {
			let entry = entry?;
			results.push(migration_event_from_row(&entry.row));
		}
		Ok(results)
	}
}
