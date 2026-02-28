// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::migration::MigrationDef, key::migration::MigrationKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::migration::migration_def_from_row};

impl CatalogStore {
	pub(crate) fn find_migration_by_name(txn: &mut Transaction<'_>, name: &str) -> Result<Option<MigrationDef>> {
		// Scan all migrations and find by name
		let range = MigrationKey::full_scan();
		for entry in txn.range(range, 1024)? {
			let entry = entry?;
			let def = migration_def_from_row(&entry.values);
			if def.name == name {
				return Ok(Some(def));
			}
		}
		Ok(None)
	}
}
