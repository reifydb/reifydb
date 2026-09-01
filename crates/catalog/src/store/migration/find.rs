// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{interface::catalog::migration::Migration, key::system::MigrationKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::migration::migration_from_row};

impl CatalogStore {
	pub(crate) fn find_migration_by_name(txn: &mut Transaction<'_>, name: &str) -> Result<Option<Migration>> {
		let range = MigrationKey::full_scan();
		for entry in txn.range(range, RangeScope::All, 1024)? {
			let entry = entry?;
			let def = migration_from_row(EncodedCatalogRow::view(&entry.bytes));
			if def.name == name {
				return Ok(Some(def));
			}
		}
		Ok(None)
	}
}
