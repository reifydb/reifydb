// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, shape::ShapeId},
	row::Ttl,
};
use reifydb_store_multi::gc::row::ListRowTtls;

use crate::catalog::Catalog;

impl ListRowTtls for Catalog {
	fn list_row_ttls(&self) -> Vec<(ShapeId, Ttl)> {
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

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
