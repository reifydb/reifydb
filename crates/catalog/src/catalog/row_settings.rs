// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::shape::ShapeId, row::RowSettings};
use reifydb_store_multi::flush::ShapePersistence;
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	pub fn find_row_settings(&self, txn: &mut Transaction<'_>, shape: ShapeId) -> Result<Option<RowSettings>> {
		if let Some(settings) = self.cache.find_row_settings_at(shape, txn.version()) {
			return Ok(Some(settings));
		}
		if let Some(settings) = CatalogStore::find_row_settings(txn, shape)? {
			warn!("row settings for {:?} found in storage but not in CatalogCache", shape);
			return Ok(Some(settings));
		}
		Ok(None)
	}

	pub fn list_row_settings(&self) -> Vec<(ShapeId, RowSettings)> {
		self.cache
			.row_settings
			.iter()
			.filter_map(|entry| {
				let shape = *entry.key();
				let settings = entry.value().get_latest()?;
				Some((shape, settings))
			})
			.collect()
	}
}

impl ShapePersistence for Catalog {
	fn is_persistent(&self, shape: ShapeId) -> bool {
		self.cache.find_row_settings(shape).is_none_or(|s| s.persistent)
	}
}
