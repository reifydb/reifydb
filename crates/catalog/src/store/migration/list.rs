// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::migration::{MigrationDef, MigrationEvent},
	key::{migration::MigrationKey, migration_event::MigrationEventKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn list_migrations(txn: &mut Transaction<'_>) -> crate::Result<Vec<MigrationDef>> {
		let range = MigrationKey::full_scan();
		let mut results = Vec::new();
		for entry in txn.range(range, 1024)? {
			let entry = entry?;
			results.push(super::migration_def_from_row(&entry.values));
		}
		Ok(results)
	}

	pub(crate) fn list_migration_events(txn: &mut Transaction<'_>) -> crate::Result<Vec<MigrationEvent>> {
		let range = MigrationEventKey::full_scan();
		let mut results = Vec::new();
		for entry in txn.range(range, 1024)? {
			let entry = entry?;
			results.push(super::migration_event_from_row(&entry.values));
		}
		Ok(results)
	}
}
