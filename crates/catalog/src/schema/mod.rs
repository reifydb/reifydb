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
	encoded::{key::EncodedKey, SchemaFingerprint},
	key::{SchemaFieldKey, SchemaKey},
	schema::{Schema, SchemaField},
};
use reifydb_transaction::single::TransactionSingle;
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::store::schema as schema_store;

/// Thread-safe schema registry with content-addressable caching.
///
/// Schemas are stored by their fingerprint (a hash of their field definitions).
/// The same field configuration always produces the same fingerprint, enabling
/// deduplication of identical schemas.
#[derive(Clone)]
pub struct SchemaRegistry(Arc<SchemaRegistryInner>);

struct SchemaRegistryInner {
	single: TransactionSingle,
	/// Cache of schemas by fingerprint
	cache: SkipMap<SchemaFingerprint, Arc<Schema>>,
	/// Write lock for serializing creates
	write_lock: Mutex<()>,
}

/// Compute all storage keys for a schema.
///
/// Single-version transactions require upfront key declaration for lock ordering.
/// This computes the header key and all field keys for a given schema.
fn compute_schema_keys(fingerprint: SchemaFingerprint, field_count: usize) -> Vec<EncodedKey> {
	let mut keys = Vec::with_capacity(1 + field_count);

	// Schema header key
	keys.push(SchemaKey::encoded(fingerprint));

	// Schema field keys
	for idx in 0..field_count {
		keys.push(SchemaFieldKey::encoded(fingerprint, idx as u16));
	}

	keys
}

impl SchemaRegistry {
	/// Create a new empty schema registry.
	pub fn new(single: TransactionSingle) -> Self {
		Self(Arc::new(SchemaRegistryInner {
			single,
			cache: SkipMap::new(),
			write_lock: Mutex::new(()),
		}))
	}

	pub fn testing() -> Self {
		Self::new(TransactionSingle::testing())
	}

	/// Get an existing schema by fingerprint, or create and persist a new one.
	///
	/// This method is thread-safe with the following guarantees:
	/// - Cache reads are lock-free (via SkipMap)
	/// - Creates are serialized via write_lock
	/// - Double-check pattern prevents duplicate creates
	pub fn get_or_create(&self, fields: Vec<SchemaField>) -> crate::Result<Arc<Schema>> {
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

		// Compute keys for transaction (header + all fields)
		let keys = compute_schema_keys(fingerprint, schema.field_count());

		// Begin single-version command transaction
		let mut cmd = self.0.single.begin_command(&keys)?;

		// Check storage (handles cache-cleared-but-exists-in-storage case)
		if let Some(stored_schema) = schema_store::find_schema_by_fingerprint(&mut cmd, fingerprint)? {
			let arc_schema = Arc::new(stored_schema);
			self.0.cache.insert(fingerprint, arc_schema.clone());
			// No commit needed for read-only path, just drop transaction
			return Ok(arc_schema);
		}

		// Create new schema and persist
		let arc_schema = Arc::new(schema);
		schema_store::create_schema(&mut cmd, &arc_schema)?;

		// Commit the transaction
		cmd.commit()?;

		// Cache only after successful commit
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
	///
	/// This method accepts an external transaction for reading schemas.
	/// For creating new schemas, use `get_or_create()` instead.
	pub fn get_or_load(
		&self,
		fingerprint: SchemaFingerprint,
		txn: &mut impl IntoStandardTransaction,
	) -> crate::Result<Option<Arc<Schema>>> {
		use crate::store::schema::schema::{schema_field, schema_header};
		use reifydb_core::key::{SchemaFieldKey, SchemaKey};
		use reifydb_type::{
			error::{diagnostic::internal::internal, Error},
			value::constraint::{FFITypeConstraint, TypeConstraint},
		};

		// Check cache first
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(Some(entry.value().clone()));
		}

		// Check storage (inlined from find_schema_by_fingerprint for StandardTransaction)
		let mut std_txn = txn.into_standard_transaction();

		// Read schema header
		let header_key = SchemaKey::encoded(fingerprint);
		let header_entry = match std_txn.get(&header_key)? {
			Some(entry) => entry,
			None => return Ok(None),
		};

		let field_count =
			schema_header::SCHEMA.get_u16(&header_entry.values, schema_header::FIELD_COUNT) as usize;

		let mut fields = Vec::with_capacity(field_count);
		for i in 0..field_count {
			let field_key = SchemaFieldKey::encoded(fingerprint, i as u16);
			let field_entry = std_txn.get(&field_key)?.ok_or_else(|| {
				Error(internal(format!("Schema field {} missing for fingerprint {:?}", i, fingerprint)))
			})?;

			let name = schema_field::SCHEMA.get_utf8(&field_entry.values, schema_field::NAME).to_string();
			let base_type = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::BASE_TYPE);
			let constraint_type =
				schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P1);
			let constraint_param2 =
				schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P2);
			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::OFFSET);
			let size = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::SIZE);
			let align = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::ALIGN);

			fields.push(SchemaField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		let schema = Schema::from_parts(fingerprint, fields);
		let arc_schema = Arc::new(schema);
		self.0.cache.insert(fingerprint, arc_schema.clone());

		Ok(Some(arc_schema))
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

impl std::fmt::Debug for SchemaRegistry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SchemaRegistry").field("cache_size", &self.0.cache.len()).finish_non_exhaustive()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn test_schema_registry_caching() {
		let registry = SchemaRegistry::new(TransactionSingle::testing());

		let fields = vec![
			SchemaField::unconstrained("id", Type::Int8),
			SchemaField::unconstrained("name", Type::Utf8),
		];

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
		let registry = SchemaRegistry::new(TransactionSingle::testing());

		let fields = vec![SchemaField::unconstrained("x", Type::Float8)];
		let schema = Arc::new(Schema::new(fields));
		let fingerprint = schema.fingerprint();

		registry.cache_schema(schema);

		// Should find it in cache
		assert!(registry.get(fingerprint).is_some());

		// Unknown fingerprint should return None
		assert!(registry.get(SchemaFingerprint::new(0xDEADBEEF)).is_none());
	}
}
