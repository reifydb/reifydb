// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::Ttl,
};
use reifydb_store_multi::gc::operator::ListOperatorTtls;

use crate::catalog::Catalog;

impl ListOperatorTtls for Catalog {
	fn list_operator_ttls(&self) -> Vec<(FlowNodeId, Ttl)> {
		self.cache
			.operator_ttls
			.iter()
			.filter_map(|entry| {
				let node = *entry.key();
				let ttl = entry.value().get_latest()?;
				Some((node, ttl))
			})
			.collect()
	}

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
