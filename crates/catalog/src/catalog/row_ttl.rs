// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{config::SystemConfig, interface::catalog::shape::ShapeId, row::RowTtl};
use reifydb_store_multi::ttl::ListRowTtls;

use crate::catalog::Catalog;

impl ListRowTtls for Catalog {
	fn list_row_ttls(&self) -> Vec<(ShapeId, RowTtl)> {
		// Ideally this should fall back to reading from storage if not loaded in materialized,
		// but ListRowTtls does not provide a Transaction context required by CatalogStore.
		// For now we read directly from materialized which is populated on startup.
		self.materialized
			.row_ttls
			.iter()
			.filter_map(|entry| {
				let shape = *entry.key();
				let ttl = entry.value().get_latest()?;
				Some((shape, ttl))
			})
			.collect()
	}

	fn system_config(&self) -> SystemConfig {
		self.materialized.system_config()
	}
}
