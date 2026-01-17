// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema Registry for content-addressable schema storage.
//!
//! The SchemaRegistry provides:
//! - In-memory caching of schemas by fingerprint
//! - Thread-safe access for concurrent reads
//! - Single-writer semantics for creates

pub mod load;

use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	encoded::SchemaFingerprint,
	schema::{Schema, SchemaField},
};
use reifydb_transaction::standard::{IntoStandardTransaction, command::StandardCommandTransaction};

use crate::store::schema as schema_store;

/// Thread-safe schema registry with content-addressable caching.
///
/// Schemas are stored by their fingerprint (a hash of their field definitions).
/// The same field configuration always produces the same fingerprint, enabling
/// deduplication of identical schemas.
#[derive(Debug, Clone)]
pub struct SchemaRegistry(Arc<SchemaRegistryInner>);

#[derive(Debug)]
struct SchemaRegistryInner {
	/// Cache of schemas by fingerprint
	cache: SkipMap<SchemaFingerprint, Arc<Schema>>,
	/// Write lock for serializing creates
	write_lock: Mutex<()>,
}

impl Default for SchemaRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl SchemaRegistry {
	/// Create a new empty schema registry.
	pub fn new() -> Self {
		Self(Arc::new(SchemaRegistryInner {
			cache: SkipMap::new(),
			write_lock: Mutex::new(()),
		}))
	}

	/// Get an existing schema by fingerprint, or create and persist a new one.
	///
	/// This method is thread-safe with the following guarantees:
	/// - Cache reads are lock-free (via SkipMap)
	/// - Creates are serialized via write_lock
	/// - Double-check pattern prevents duplicate creates
	pub fn get_or_create(
		&self,
		fields: Vec<SchemaField>,
		cmd: &mut StandardCommandTransaction,
	) -> crate::Result<Arc<Schema>> {
		let schema = Schema::new(fields);
		let fingerprint = schema.fingerprint();

		// Fast path: cache hit (lock-free)
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		// Slow path: acquire write lock
		let _guard = self.0.write_lock.lock().unwrap();

		// Double-check after acquiring lock
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		// Check storage (handles cache-cleared-but-exists-in-storage case)
		if let Some(stored_schema) = schema_store::find_schema_by_fingerprint(cmd, fingerprint)? {
			let arc_schema = Arc::new(stored_schema);
			self.0.cache.insert(fingerprint, arc_schema.clone());
			return Ok(arc_schema);
		}

		// Create new schema and persist
		let arc_schema = Arc::new(schema);
		schema_store::create_schema(cmd, &arc_schema)?;

		// Cache it
		self.0.cache.insert(fingerprint, arc_schema.clone());

		Ok(arc_schema)
	}

	/// Look up a schema by fingerprint (cache only).
	///
	/// Returns None if the schema is not in the cache.
	pub fn get(&self, fingerprint: SchemaFingerprint) -> Option<Arc<Schema>> {
		self.0.cache.get(&fingerprint).map(|entry| entry.value().clone())
	}

	/// Look up a schema by fingerprint, checking storage if not cached.
	pub fn get_or_load(
		&self,
		fingerprint: SchemaFingerprint,
		txn: &mut impl IntoStandardTransaction,
	) -> crate::Result<Option<Arc<Schema>>> {
		// Check cache first
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(Some(entry.value().clone()));
		}

		// Check storage
		if let Some(schema) = schema_store::find_schema_by_fingerprint(txn, fingerprint)? {
			let arc_schema = Arc::new(schema);
			self.0.cache.insert(fingerprint, arc_schema.clone());
			return Ok(Some(arc_schema));
		}

		Ok(None)
	}

	/// Insert a schema into the cache (used by loader during startup).
	///
	/// This does NOT persist the schema - it assumes it already exists in storage.
	pub(crate) fn cache_schema(&self, schema: Arc<Schema>) {
		self.0.cache.insert(schema.fingerprint(), schema);
	}

	/// Get the number of cached schemas.
	pub fn cache_size(&self) -> usize {
		self.0.cache.len()
	}

	/// Clear the cache (useful for testing).
	#[cfg(test)]
	pub fn clear_cache(&self) {
		self.0.cache.clear();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn test_schema_registry_caching() {
		let registry = SchemaRegistry::new();

		let fields = vec![SchemaField::new("id", Type::Int8), SchemaField::new("name", Type::Utf8)];

		// Create schema and insert into cache manually for testing
		let schema = Arc::new(Schema::new(fields));
		registry.cache_schema(schema.clone());

		// Should find it in cache
		let cached = registry.get(schema.fingerprint());
		assert!(cached.is_some());
		assert!(Arc::ptr_eq(&schema, &cached.unwrap()));

		// Cache should have one entry
		assert_eq!(registry.cache_size(), 1);
	}

	#[test]
	fn test_schema_registry_get() {
		let registry = SchemaRegistry::new();

		let fields = vec![SchemaField::new("x", Type::Float8)];
		let schema = Arc::new(Schema::new(fields));
		let fingerprint = schema.fingerprint();

		registry.cache_schema(schema);

		// Should find it in cache
		assert!(registry.get(fingerprint).is_some());

		// Unknown fingerprint should return None
		assert!(registry.get(SchemaFingerprint::new(0xDEADBEEF)).is_none());
	}
}
