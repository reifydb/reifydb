// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::object::ObjectId, row::RowSettings};
use reifydb_store_multi::flush::ObjectPersistence;
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	pub fn find_row_settings(&self, txn: &mut Transaction<'_>, object: ObjectId) -> Result<Option<RowSettings>> {
		if let Some(settings) = self.cache.find_row_settings_at(object, txn.version()) {
			return Ok(Some(settings));
		}
		if let Some(settings) = CatalogStore::find_row_settings(txn, object)? {
			warn!("row settings for {:?} found in storage but not in CatalogCache", object);
			return Ok(Some(settings));
		}
		Ok(None)
	}

	pub fn find_row_settings_latest(&self, object: ObjectId) -> Option<RowSettings> {
		self.cache.find_row_settings(object)
	}

	pub fn list_row_settings(&self) -> Vec<(ObjectId, RowSettings)> {
		self.cache
			.row_settings
			.iter()
			.filter_map(|entry| {
				let object = *entry.key();
				let settings = entry.value().get_latest()?;
				Some((object, settings))
			})
			.collect()
	}
}

impl ObjectPersistence for Catalog {
	fn is_persistent(&self, object: ObjectId) -> bool {
		self.cache.find_row_settings(object).is_none_or(|s| s.persistent)
	}
}
