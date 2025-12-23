// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog cache for the flow consumer.
//!
//! Caches source metadata (columns, types, dictionaries) to avoid redundant catalog lookups
//! during CDC processing. The cache is invalidated when schema changes are observed via CDC.

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::{
	CatalogStore, CatalogViewQueryOperations,
	resolve::{resolve_ringbuffer, resolve_table, resolve_view},
	transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations, CatalogTableQueryOperations},
};
use reifydb_core::{
	Result,
	interface::{ColumnDef, DictionaryDef, QueryTransaction, SourceId},
};
use reifydb_type::Type;
use tokio::sync::RwLock;

/// Pre-computed metadata for a source, avoiding repeated catalog lookups.
///
/// Contains all information needed to decode row bytes into `Row` values,
/// including storage types, value types, and dictionary definitions.
pub struct SourceMetadata {
	pub storage_types: Vec<Type>,
	pub value_types: Vec<(String, Type)>,
	pub dictionaries: Vec<Option<DictionaryDef>>,
	pub has_dictionary_columns: bool,
}

/// Thread-safe cache for source metadata.
///
/// Caches column definitions, type layouts, and dictionary info per SourceId.
///
/// # Thread Safety
///
/// Uses `tokio::sync::RwLock` for async-safe concurrent access:
/// - Read path: Multiple tasks can read cached metadata concurrently
/// - Write path: Single writer for cache updates
pub struct FlowCatalog {
	sources: RwLock<HashMap<SourceId, Arc<SourceMetadata>>>,
}

impl FlowCatalog {
	pub fn new() -> Self {
		Self {
			sources: RwLock::new(HashMap::new()),
		}
	}

	/// Get cached metadata or load from catalog on cache miss.
	///
	/// Uses double-check locking pattern:
	/// 1. Fast path: read lock check for cached entry
	/// 2. Slow path: write lock, re-check, then load and cache
	pub async fn get_or_load<T>(&self, txn: &mut T, source: SourceId) -> Result<Arc<SourceMetadata>>
	where
		T: CatalogTableQueryOperations
			+ CatalogNamespaceQueryOperations
			+ CatalogRingBufferQueryOperations
			+ CatalogViewQueryOperations
			+ QueryTransaction,
	{
		// Fast path: read lock check
		{
			let cache = self.sources.read().await;
			if let Some(metadata) = cache.get(&source) {
				return Ok(Arc::clone(metadata));
			}
		}

		// Slow path: load and cache
		let metadata = Arc::new(self.load_source_metadata(txn, source).await?);
		let mut cache = self.sources.write().await;
		Ok(Arc::clone(cache.entry(source).or_insert(metadata)))
	}

	async fn load_source_metadata<T>(&self, txn: &mut T, source: SourceId) -> Result<SourceMetadata>
	where
		T: CatalogTableQueryOperations
			+ CatalogNamespaceQueryOperations
			+ CatalogViewQueryOperations
			+ CatalogRingBufferQueryOperations
			+ QueryTransaction,
	{
		// Get columns based on source type
		let columns: Vec<ColumnDef> = match source {
			SourceId::Table(table_id) => resolve_table(txn, table_id).await?.def().columns.clone(),
			SourceId::View(view_id) => resolve_view(txn, view_id).await?.def().columns.clone(),
			SourceId::RingBuffer(rb_id) => resolve_ringbuffer(txn, rb_id).await?.def().columns.clone(),
			SourceId::Flow(_) => unimplemented!("Flow sources not supported in flows"),
			SourceId::TableVirtual(_) => unimplemented!("Virtual table sources not supported in flows"),
			SourceId::Dictionary(_) => unimplemented!("Dictionary sources not supported in flows"),
		};

		// Build type info and dictionary info
		let mut storage_types = Vec::with_capacity(columns.len());
		let mut value_types = Vec::with_capacity(columns.len());
		let mut dictionaries = Vec::with_capacity(columns.len());
		let mut has_dictionary_columns = false;

		for col in &columns {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = CatalogStore::find_dictionary(txn, dict_id).await? {
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

		Ok(SourceMetadata {
			storage_types,
			value_types,
			dictionaries,
			has_dictionary_columns,
		})
	}
}

impl Default for FlowCatalog {
	fn default() -> Self {
		Self::new()
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

	#[tokio::test]
	async fn test_new_creates_empty_cache() {
		let catalog = FlowCatalog::new();
		assert!(catalog.sources.read().await.is_empty());
	}

	#[tokio::test]
	async fn test_default() {
		let catalog = FlowCatalog::default();
		assert!(catalog.sources.read().await.is_empty());
	}

	#[tokio::test]
	async fn test_get_or_load_table() {
		let mut txn = create_test_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::Table(table.id)).await.unwrap();

		// The test table has no columns, so metadata should reflect that
		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(metadata.dictionaries.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	#[tokio::test]
	async fn test_get_or_load_cache_hit() {
		let mut txn = create_test_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		let catalog = FlowCatalog::new();
		let source = SourceId::Table(table.id);

		let first = catalog.get_or_load(&mut txn, source).await.unwrap();
		let second = catalog.get_or_load(&mut txn, source).await.unwrap();

		// Should return the same Arc (cache hit)
		assert!(Arc::ptr_eq(&first, &second));
	}

	#[tokio::test]
	async fn test_get_or_load_view() {
		let mut txn = create_test_transaction().await;
		ensure_test_namespace(&mut txn).await;
		let view = create_view(&mut txn, "test_namespace", "test_view", &[]).await;

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::View(view.id)).await.unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	#[tokio::test]
	async fn test_get_or_load_ringbuffer() {
		let mut txn = create_test_transaction().await;
		let rb = ensure_test_ringbuffer(&mut txn).await;

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::RingBuffer(rb.id)).await.unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}
}
