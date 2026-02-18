// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog cache for the flow consumer.
//!
//! Caches source metadata (columns, types, dictionaries) to avoid redundant catalog lookups
//! during CDC processing. The cache is invalidated when schema changes are observed via CDC.

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

pub struct FlowCatalog {
	catalog: Catalog,
	flows: RwLock<HashMap<FlowId, FlowDag>>,
}

impl FlowCatalog {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			catalog,
			flows: RwLock::new(HashMap::new()),
		}
	}

	/// Get or load flow from catalog with caching (double-check locking pattern).
	/// Returns (FlowDag, is_new) where is_new is true if the flow was newly cached.
	pub fn get_or_load_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<(FlowDag, bool)> {
		// Fast path: read lock - flow already cached
		{
			let cache = self.flows.read();
			if let Some(flow) = cache.get(&flow_id) {
				return Ok((flow.clone(), false));
			}
		}

		// Slow path: load and cache
		let flow = load_flow_dag(&self.catalog, txn, flow_id)?;
		let mut cache = self.flows.write();

		let is_new = !cache.contains_key(&flow_id);
		let cached_flow = cache.entry(flow_id).or_insert(flow).clone();

		Ok((cached_flow, is_new))
	}

	/// Get all registered flow IDs
	pub fn get_flow_ids(&self) -> Vec<FlowId> {
		self.flows.read().keys().copied().collect()
	}
}

impl Clone for FlowCatalog {
	fn clone(&self) -> Self {
		Self {
			catalog: self.catalog.clone(),
			flows: RwLock::new(HashMap::new()),
		}
	}
}
