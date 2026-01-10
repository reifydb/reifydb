// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog cache for the flow consumer.
//!
//! Caches source metadata (columns, types, dictionaries) to avoid redundant catalog lookups
//! during CDC processing. The cache is invalidated when schema changes are observed via CDC.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use reifydb_catalog::Catalog;
use reifydb_core::{
	Result,
	interface::{ColumnDef, DictionaryDef, FlowId, PrimitiveId},
};
use reifydb_rql::flow::{FlowDag, load_flow_dag};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Type;

/// Pre-computed metadata for a source, avoiding repeated catalog lookups.
///
/// Contains all information needed to decode row bytes into `Row` values,
/// including storage types, value types, and dictionary definitions.
pub struct PrimitiveMetadata {
	pub storage_types: Vec<Type>,
	pub value_types: Vec<(String, Type)>,
	pub dictionaries: Vec<Option<DictionaryDef>>,
	pub has_dictionary_columns: bool,
}

/// Thread-safe cache for source metadata.
///
/// Caches column definitions, type layouts, and dictionary info per PrimitiveId.
///
/// # Thread Safety
///
/// Uses `tokio::sync::RwLock` for async-safe concurrent access:
/// - Read path: Multiple tasks can read cached metadata concurrently
/// - Write path: Single writer for cache updates
pub struct FlowCatalog {
	catalog: Catalog,
	sources: RwLock<HashMap<PrimitiveId, Arc<PrimitiveMetadata>>>,
	flows: RwLock<HashMap<FlowId, FlowDag>>,
}

impl FlowCatalog {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			catalog,
			sources: RwLock::new(HashMap::new()),
			flows: RwLock::new(HashMap::new()),
		}
	}

	/// Get cached metadata or load from catalog on cache miss.
	///
	/// Uses double-check locking pattern:
	/// 1. Fast path: read lock check for cached entry
	/// 2. Slow path: write lock, re-check, then load and cache
	pub fn get_or_load<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		source: PrimitiveId,
	) -> Result<Arc<PrimitiveMetadata>> {
		// Fast path: read lock check
		{
			let cache = self.sources.read();
			if let Some(metadata) = cache.get(&source) {
				return Ok(Arc::clone(metadata));
			}
		}

		// Slow path: load and cache
		let metadata = Arc::new(self.load_primitive_metadata(txn, source)?);
		let mut cache = self.sources.write();
		Ok(Arc::clone(cache.entry(source).or_insert(metadata)))
	}

	fn load_primitive_metadata<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		source: PrimitiveId,
	) -> Result<PrimitiveMetadata> {
		// Get columns based on source type
		let columns: Vec<ColumnDef> = match source {
			PrimitiveId::Table(table_id) => {
				self.catalog.resolve_table(txn, table_id)?.def().columns.clone()
			}
			PrimitiveId::View(view_id) => self.catalog.resolve_view(txn, view_id)?.def().columns.clone(),
			PrimitiveId::RingBuffer(rb_id) => {
				self.catalog.resolve_ringbuffer(txn, rb_id)?.def().columns.clone()
			}
			PrimitiveId::Flow(_) => unimplemented!("Flow sources not supported in flows"),
			PrimitiveId::TableVirtual(_) => unimplemented!("Virtual table sources not supported in flows"),
			PrimitiveId::Dictionary(_) => unimplemented!("Dictionary sources not supported in flows"),
		};

		// Build type info and dictionary info
		let mut storage_types = Vec::with_capacity(columns.len());
		let mut value_types = Vec::with_capacity(columns.len());
		let mut dictionaries = Vec::with_capacity(columns.len());
		let mut has_dictionary_columns = false;

		for col in &columns {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = self.catalog.find_dictionary(txn, dict_id)? {
					storage_types.push(dict.id_type);
					value_types.push((col.name.clone(), dict.value_type));
					dictionaries.push(Some(dict));
					has_dictionary_columns = true;
				} else {
					// Dictionary not found, fall back to constraint type
					storage_types.push(col.constraint.get_type());
					value_types.push((col.name.clone(), col.constraint.get_type()));
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				value_types.push((col.name.clone(), col.constraint.get_type()));
				dictionaries.push(None);
			}
		}

		Ok(PrimitiveMetadata {
			storage_types,
			value_types,
			dictionaries,
			has_dictionary_columns,
		})
	}

	/// Get or load flow from catalog with caching (double-check locking pattern).
	/// Returns (FlowDag, is_new) where is_new is true if the flow was newly cached.
	pub fn get_or_load_flow<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		flow_id: FlowId,
	) -> Result<(FlowDag, bool)> {
		// Fast path: read lock - flow already cached
		{
			let cache = self.flows.read();
			if let Some(flow) = cache.get(&flow_id) {
				return Ok((flow.clone(), false));
			}
		}

		// Slow path: load and cache
		let flow = load_flow_dag(txn, flow_id)?;
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
			sources: RwLock::new(HashMap::new()),
			flows: RwLock::new(HashMap::new()),
		}
	}
}

impl Default for FlowCatalog {
	fn default() -> Self {
		Self::new(Catalog::default())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_catalog::test_utils::{
		create_view, ensure_test_namespace, ensure_test_ringbuffer, ensure_test_table,
	};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	#[test]
	fn test_new_creates_empty_cache() {
		let flow_catalog = FlowCatalog::default();
		assert!(flow_catalog.sources.read().is_empty());
	}

	#[test]
	fn test_default() {
		let flow_catalog = FlowCatalog::default();
		assert!(flow_catalog.sources.read().is_empty());
	}

	#[test]
	fn test_get_or_load_table() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let flow_catalog = FlowCatalog::default();
		let metadata = flow_catalog.get_or_load(&mut txn, PrimitiveId::Table(table.id)).unwrap();

		// The test table has no columns, so metadata should reflect that
		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(metadata.dictionaries.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	#[test]
	fn test_get_or_load_cache_hit() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let flow_catalog = FlowCatalog::default();
		let source = PrimitiveId::Table(table.id);

		let first = flow_catalog.get_or_load(&mut txn, source).unwrap();
		let second = flow_catalog.get_or_load(&mut txn, source).unwrap();

		// Should return the same Arc (cache hit)
		assert!(Arc::ptr_eq(&first, &second));
	}

	#[test]
	fn test_get_or_load_view() {
		let mut txn = create_test_transaction();
		ensure_test_namespace(&mut txn);
		let view = create_view(&mut txn, "test_namespace", "test_view", &[]);

		let flow_catalog = FlowCatalog::default();
		let metadata = flow_catalog.get_or_load(&mut txn, PrimitiveId::View(view.id)).unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	#[test]
	fn test_get_or_load_ringbuffer() {
		let mut txn = create_test_transaction();
		let rb = ensure_test_ringbuffer(&mut txn);

		let flow_catalog = FlowCatalog::default();
		let metadata = flow_catalog.get_or_load(&mut txn, PrimitiveId::RingBuffer(rb.id)).unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}
}
