// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::migration::Migration, key::migration::MigrationKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::migration::migration_from_row};

impl CatalogStore {
	pub(crate) fn find_migration_by_name(txn: &mut Transaction<'_>, name: &str) -> Result<Option<Migration>> {
		let range = MigrationKey::full_scan();
		for entry in txn.range(range, RangeScope::All, 1024)? {
			let entry = entry?;
			let def = migration_from_row(&entry.row);
			if def.name == name {
				return Ok(Some(def));
			}
		}
		Ok(None)
	}
}
