// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, shape::ShapeId},
	row::RowSettings,
};
use reifydb_store_multi::{flush::ShapePersistence, gc::row::ListRowSettings};
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
}

impl ListRowSettings for Catalog {
	fn list_row_settings(&self) -> Vec<(ShapeId, RowSettings)> {
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

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}

impl ShapePersistence for Catalog {
	fn is_persistent(&self, shape: ShapeId) -> bool {
		self.cache.find_row_settings(shape).is_none_or(|s| s.persistent)
	}
}
