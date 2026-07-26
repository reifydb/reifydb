// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::storage::StorageId, row::RowSettings};
use reifydb_store_multi::flush::ObjectPersistence;
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	pub fn find_row_settings(&self, txn: &mut Transaction<'_>, storage: StorageId) -> Result<Option<RowSettings>> {
		if let Some(settings) = self.cache.find_row_settings_at(storage, txn.version()) {
			return Ok(Some(settings));
		}
		if let Some(settings) = CatalogStore::find_row_settings(txn, storage)? {
			warn!("row settings for {:?} found in storage but not in CatalogCache", storage);
			return Ok(Some(settings));
		}
		Ok(None)
	}

	pub fn find_row_settings_latest(&self, storage: StorageId) -> Option<RowSettings> {
		self.cache.find_row_settings(storage)
	}

	pub fn list_row_settings(&self) -> Vec<(StorageId, RowSettings)> {
		self.cache
			.row_settings
			.iter()
			.filter_map(|entry| {
				let storage = *entry.key();
				let settings = entry.value().get_latest()?;
				Some((storage, settings))
			})
			.collect()
	}
}

impl ObjectPersistence for Catalog {
	fn is_persistent(&self, storage: StorageId) -> bool {
		self.cache.find_row_settings(storage).is_none_or(|s| s.persistent)
	}
}
