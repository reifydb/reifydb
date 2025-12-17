// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog cache for the flow consumer.
//!
//! Caches source metadata (columns, types, dictionaries) to avoid redundant catalog lookups
//! during CDC processing. The cache is invalidated when schema changes are observed via CDC.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use reifydb_catalog::{
	CatalogStore, CatalogViewQueryOperations,
	resolve::{resolve_ringbuffer, resolve_table, resolve_view},
	transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations, CatalogTableQueryOperations},
};
use reifydb_core::{
	EncodedKey, Result,
	interface::{ColumnDef, DictionaryDef, KeyKind, QueryTransaction, SourceId},
	key::Key,
};
use reifydb_type::Type;

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
/// The cache is invalidated when schema changes are observed via CDC.
///
/// # Thread Safety
///
/// Uses `parking_lot::RwLock` for efficient concurrent access:
/// - Read path: Multiple threads can read cached metadata concurrently
/// - Write path: Single writer for cache updates and invalidation
///
/// # Cache Invalidation
///
/// The cache observes CDC changes and invalidates affected entries:
/// - `KeyKind::Table` - invalidate `SourceId::Table(table_id)`
/// - `KeyKind::View` - invalidate `SourceId::View(view_id)`
/// - `KeyKind::RingBuffer` - invalidate `SourceId::RingBuffer(rb_id)`
/// - `KeyKind::Column` - invalidate the source the column belongs to
/// - `KeyKind::Dictionary` - clear entire cache (no reverse lookup available)
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
	pub fn get_or_load<T>(&self, txn: &mut T, source: SourceId) -> Result<Arc<SourceMetadata>>
	where
		T: CatalogTableQueryOperations
			+ CatalogNamespaceQueryOperations
			+ CatalogRingBufferQueryOperations
			+ CatalogViewQueryOperations
			+ QueryTransaction,
	{
		// Fast path: read lock check
		{
			let cache = self.sources.read();
			if let Some(metadata) = cache.get(&source) {
				return Ok(Arc::clone(metadata));
			}
		}

		// Slow path: load and cache
		let metadata = Arc::new(self.load_source_metadata(txn, source)?);
		let mut cache = self.sources.write();
		Ok(Arc::clone(cache.entry(source).or_insert(metadata)))
	}

	/// Invalidate cache entries based on observed CDC changes.
	///
	/// Call this before processing CDC changes to ensure cache consistency.
	/// This method checks the key kind and invalidates the appropriate cache entry.
	pub fn invalidate_from_cdc(&self, key: &EncodedKey) {
		let Some(kind) = Key::kind(key) else {
			return;
		};

		match kind {
			// Table definition changed - invalidate that table
			KeyKind::Table => {
				if let Some(Key::Table(table_key)) = Key::decode(key) {
					self.invalidate(SourceId::Table(table_key.table));
				}
			}
			// View definition changed - invalidate that view
			KeyKind::View => {
				if let Some(Key::View(view_key)) = Key::decode(key) {
					self.invalidate(SourceId::View(view_key.view));
				}
			}
			// RingBuffer definition changed - invalidate that ringbuffer
			KeyKind::RingBuffer => {
				if let Some(Key::RingBuffer(rb_key)) = Key::decode(key) {
					self.invalidate(SourceId::RingBuffer(rb_key.ringbuffer));
				}
			}
			// Column changed - invalidate the source it belongs to
			KeyKind::Column => {
				if let Some(Key::Column(col_key)) = Key::decode(key) {
					self.invalidate(col_key.source);
				}
			}
			// Dictionary changed - clear entire cache since we can't easily
			// determine which sources use this dictionary without a reverse lookup
			KeyKind::Dictionary => {
				self.clear();
			}
			_ => {}
		}
	}

	/// Invalidate a specific source from the cache.
	pub fn invalidate(&self, source: SourceId) {
		self.sources.write().remove(&source);
	}

	/// Clear all cached entries.
	pub fn clear(&self) {
		self.sources.write().clear();
	}

	fn load_source_metadata<T>(&self, txn: &mut T, source: SourceId) -> Result<SourceMetadata>
	where
		T: CatalogTableQueryOperations
			+ CatalogNamespaceQueryOperations
			+ CatalogViewQueryOperations
			+ CatalogRingBufferQueryOperations
			+ QueryTransaction,
	{
		// Get columns based on source type
		let columns: Vec<ColumnDef> = match source {
			SourceId::Table(table_id) => resolve_table(txn, table_id)?.def().columns.clone(),
			SourceId::View(view_id) => resolve_view(txn, view_id)?.def().columns.clone(),
			SourceId::RingBuffer(rb_id) => resolve_ringbuffer(txn, rb_id)?.def().columns.clone(),
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
				if let Some(dict) = CatalogStore::find_dictionary(txn, dict_id)? {
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
	use reifydb_core::{
		interface::ColumnId,
		key::{ColumnKey, DictionaryKey, EncodableKey, RingBufferKey, TableKey, ViewKey},
	};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	// Basic construction tests

	#[test]
	fn test_new_creates_empty_cache() {
		let catalog = FlowCatalog::new();
		assert!(catalog.sources.read().is_empty());
	}

	#[test]
	fn test_default() {
		let catalog = FlowCatalog::default();
		assert!(catalog.sources.read().is_empty());
	}

	// Cache operations tests

	#[test]
	fn test_get_or_load_table() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::Table(table.id)).unwrap();

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

		let catalog = FlowCatalog::new();
		let source = SourceId::Table(table.id);

		let first = catalog.get_or_load(&mut txn, source).unwrap();
		let second = catalog.get_or_load(&mut txn, source).unwrap();

		// Should return the same Arc (cache hit)
		assert!(Arc::ptr_eq(&first, &second));
	}

	#[test]
	fn test_get_or_load_view() {
		let mut txn = create_test_transaction();
		ensure_test_namespace(&mut txn);
		let view = create_view(&mut txn, "test_namespace", "test_view", &[]);

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::View(view.id)).unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	#[test]
	fn test_get_or_load_ringbuffer() {
		let mut txn = create_test_transaction();
		let rb = ensure_test_ringbuffer(&mut txn);

		let catalog = FlowCatalog::new();
		let metadata = catalog.get_or_load(&mut txn, SourceId::RingBuffer(rb.id)).unwrap();

		assert!(metadata.storage_types.is_empty());
		assert!(metadata.value_types.is_empty());
		assert!(!metadata.has_dictionary_columns);
	}

	// CDC invalidation tests

	#[test]
	fn test_invalidate_from_cdc_table_key() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let catalog = FlowCatalog::new();
		let source = SourceId::Table(table.id);

		// Load into cache
		let _ = catalog.get_or_load(&mut txn, source).unwrap();
		assert!(!catalog.sources.read().is_empty());

		// Invalidate via CDC key
		let key = TableKey {
			table: table.id,
		}
		.encode();
		catalog.invalidate_from_cdc(&key);

		// Cache should be empty for this source
		assert!(catalog.sources.read().get(&source).is_none());
	}

	#[test]
	fn test_invalidate_from_cdc_view_key() {
		let mut txn = create_test_transaction();
		ensure_test_namespace(&mut txn);
		let view = create_view(&mut txn, "test_namespace", "test_view_cdc", &[]);

		let catalog = FlowCatalog::new();
		let source = SourceId::View(view.id);

		// Load into cache
		let _ = catalog.get_or_load(&mut txn, source).unwrap();
		assert!(catalog.sources.read().get(&source).is_some());

		// Invalidate via CDC key
		let key = ViewKey {
			view: view.id,
		}
		.encode();
		catalog.invalidate_from_cdc(&key);

		assert!(catalog.sources.read().get(&source).is_none());
	}

	#[test]
	fn test_invalidate_from_cdc_ringbuffer_key() {
		let mut txn = create_test_transaction();
		let rb = ensure_test_ringbuffer(&mut txn);

		let catalog = FlowCatalog::new();
		let source = SourceId::RingBuffer(rb.id);

		// Load into cache
		let _ = catalog.get_or_load(&mut txn, source).unwrap();
		assert!(catalog.sources.read().get(&source).is_some());

		// Invalidate via CDC key
		let key = RingBufferKey::encoded(rb.id);
		catalog.invalidate_from_cdc(&key);

		assert!(catalog.sources.read().get(&source).is_none());
	}

	#[test]
	fn test_invalidate_from_cdc_column_key() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let catalog = FlowCatalog::new();
		let source = SourceId::Table(table.id);

		// Load into cache
		let _ = catalog.get_or_load(&mut txn, source).unwrap();
		assert!(catalog.sources.read().get(&source).is_some());

		// Invalidate via column key (should invalidate the source the column belongs to)
		let key = ColumnKey {
			source,
			column: ColumnId(1),
		}
		.encode();
		catalog.invalidate_from_cdc(&key);

		assert!(catalog.sources.read().get(&source).is_none());
	}

	#[test]
	fn test_invalidate_from_cdc_dictionary_key() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);
		let rb = ensure_test_ringbuffer(&mut txn);

		let catalog = FlowCatalog::new();

		// Load multiple sources into cache
		let _ = catalog.get_or_load(&mut txn, SourceId::Table(table.id)).unwrap();
		let _ = catalog.get_or_load(&mut txn, SourceId::RingBuffer(rb.id)).unwrap();
		assert_eq!(catalog.sources.read().len(), 2);

		// Invalidate via dictionary key (should clear entire cache)
		let key = DictionaryKey {
			dictionary: reifydb_core::interface::DictionaryId(1),
		}
		.encode();
		catalog.invalidate_from_cdc(&key);

		assert!(catalog.sources.read().is_empty());
	}

	// Direct invalidation tests

	#[test]
	fn test_invalidate_removes_entry() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);

		let catalog = FlowCatalog::new();
		let source = SourceId::Table(table.id);

		// Load into cache
		let _ = catalog.get_or_load(&mut txn, source).unwrap();
		assert!(catalog.sources.read().get(&source).is_some());

		// Direct invalidation
		catalog.invalidate(source);

		assert!(catalog.sources.read().get(&source).is_none());
	}

	#[test]
	fn test_invalidate_nonexistent() {
		let catalog = FlowCatalog::new();

		// Should not panic when invalidating non-existent entry
		catalog.invalidate(SourceId::Table(reifydb_core::interface::TableId(999)));
	}

	#[test]
	fn test_clear() {
		let mut txn = create_test_transaction();
		let table = ensure_test_table(&mut txn);
		let rb = ensure_test_ringbuffer(&mut txn);

		let catalog = FlowCatalog::new();

		// Load multiple sources
		let _ = catalog.get_or_load(&mut txn, SourceId::Table(table.id)).unwrap();
		let _ = catalog.get_or_load(&mut txn, SourceId::RingBuffer(rb.id)).unwrap();
		assert_eq!(catalog.sources.read().len(), 2);

		// Clear all
		catalog.clear();

		assert!(catalog.sources.read().is_empty());
	}
}
