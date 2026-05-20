// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, shape::ShapeId},
	row::Ttl,
};
use reifydb_store_multi::gc::row::ListRowTtls;
use reifydb_transaction::transaction::Transaction;

use crate::catalog::Catalog;

impl Catalog {
	pub fn find_row_ttl(&self, txn: &mut Transaction<'_>, shape: ShapeId) -> Option<Ttl> {
		self.cache.find_row_ttl_at(shape, txn.version())
	}
}

impl ListRowTtls for Catalog {
	fn list_row_ttls(&self) -> Vec<(ShapeId, Ttl)> {
		self.cache
			.row_ttls
			.iter()
			.filter_map(|entry| {
				let shape = *entry.key();
				let ttl = entry.value().get_latest()?;
				Some((shape, ttl))
			})
			.collect()
	}

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
