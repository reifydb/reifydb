// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowSchema Registry for content-addressable schema storage.
//!
//! The RowSchemaRegistry provides:
//! - In-memory caching of schemas by fingerprint
//! - Thread-safe access for concurrent reads
//! - Single-writer semantics for creates

pub mod decode;
pub mod load;

use std::{fmt, sync::Arc};

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		schema::{RowSchema, RowSchemaField, fingerprint::RowSchemaFingerprint},
	},
	error::diagnostic::internal::internal,
	key::schema::{RowSchemaFieldKey, RowSchemaKey},
};
use reifydb_transaction::{single::SingleTransaction, transaction::Transaction};
use reifydb_type::{
	error::Error,
	value::constraint::{FFITypeConstraint, TypeConstraint},
};
use tracing::{Span, field, instrument};

use crate::{
	Result,
	store::row_schema::{
		create::create_row_schema,
		find::find_row_schema_by_fingerprint,
		schema::{schema_field, schema_header},
	},
};

/// Thread-safe schema registry with content-addressable caching.
///
/// Schemas are stored by their fingerprint (a hash of their field definitions).
/// The same field configuration always produces the same fingerprint, enabling
/// deduplication of identical schemas.
#[derive(Clone)]
pub struct RowSchemaRegistry(Arc<RowSchemaRegistryInner>);

struct RowSchemaRegistryInner {
	single: SingleTransaction,
	/// Cache of schemas by fingerprint
	cache: SkipMap<RowSchemaFingerprint, RowSchema>,
}

/// Compute all storage keys for a schema.
///
/// Single-version transactions require upfront key declaration for lock ordering.
/// This computes the header key and all field keys for a given schema.
#[instrument(
	name = "row_schema_registry::compute_keys",
	level = "trace",
	skip_all,
	fields(fingerprint = ?fingerprint, key_count = field_count + 1)
)]
fn compute_schema_keys(fingerprint: RowSchemaFingerprint, field_count: usize) -> Vec<EncodedKey> {
	let mut keys = Vec::with_capacity(1 + field_count);

	// RowSchema header key
	keys.push(RowSchemaKey::encoded(fingerprint));

	// RowSchema field keys
	for idx in 0..field_count {
		keys.push(RowSchemaFieldKey::encoded(fingerprint, idx as u16));
	}

	keys
}

impl RowSchemaRegistry {
	/// Create a new empty schema registry.
	pub fn new(single: SingleTransaction) -> Self {
		Self(Arc::new(RowSchemaRegistryInner {
			single,
			cache: SkipMap::new(),
		}))
	}

	pub fn testing() -> Self {
		Self::new(SingleTransaction::testing())
	}

	/// Get an existing schema by fingerprint, or create and persist a new one.
	///
	/// This method is thread-safe with the following guarantees:
	/// - Cache reads are lock-free (via SkipMap)
	/// - Creates are serialized via write_lock
	/// - Double-check pattern prevents duplicate creates
	#[instrument(
		name = "row_schema_registry::get_or_create",
		level = "debug",
		skip(fields),
		fields(fingerprint = field::Empty, field_count = fields.len())
	)]
	pub fn get_or_create(&self, fields: Vec<RowSchemaField>) -> Result<RowSchema> {
		let schema = RowSchema::new(fields);
		let fingerprint = schema.fingerprint();
		Span::current().record("fingerprint", field::debug(&fingerprint));

		// Fast path
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		// Double-check after acquiring lock
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		let keys = compute_schema_keys(fingerprint, schema.field_count());

		let mut cmd = self.0.single.begin_command(&keys)?;

		if let Some(stored_schema) = find_row_schema_by_fingerprint(&mut cmd, fingerprint)? {
			self.0.cache.insert(fingerprint, stored_schema.clone());
			// No commit needed for read-only path, just drop transaction
			return Ok(stored_schema);
		}

		create_row_schema(&mut cmd, &schema)?;

		cmd.commit()?;

		self.0.cache.insert(fingerprint, schema.clone());

		Ok(schema)
	}

	/// Look up a schema by fingerprint (cache only).
	///
	/// Returns None if the schema is not in the cache.
	#[instrument(
		name = "row_schema_registry::get",
		level = "trace",
		fields(fingerprint = ?fingerprint)
	)]
	pub fn get(&self, fingerprint: RowSchemaFingerprint) -> Option<RowSchema> {
		self.0.cache.get(&fingerprint).map(|entry| entry.value().clone())
	}

	/// Look up a schema by fingerprint, checking storage if not cached.
	///
	/// This method accepts an external transaction for reading schemas.
	/// For creating new schemas, use `get_or_create()` instead.
	#[instrument(
		name = "row_schema_registry::get_or_load",
		level = "debug",
		skip(txn),
		fields(
			fingerprint = ?fingerprint,
			cache_hit = field::Empty,
			field_count = field::Empty
		)
	)]
	pub fn get_or_load(
		&self,
		fingerprint: RowSchemaFingerprint,
		txn: &mut Transaction<'_>,
	) -> Result<Option<RowSchema>> {
		// Check cache first
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			let schema = entry.value().clone();
			Span::current().record("cache_hit", true);
			Span::current().record("field_count", schema.field_count());
			return Ok(Some(schema));
		}

		// Read schema header
		let header_key = RowSchemaKey::encoded(fingerprint);
		let header_entry = match txn.get(&header_key)? {
			Some(entry) => entry,
			None => {
				Span::current().record("cache_hit", false);
				Span::current().record("field_count", 0);
				return Ok(None);
			}
		};

		let field_count = schema_header::SCHEMA.get_u16(&header_entry.row, schema_header::FIELD_COUNT) as usize;

		let mut fields = Vec::with_capacity(field_count);
		for i in 0..field_count {
			let field_key = RowSchemaFieldKey::encoded(fingerprint, i as u16);
			let field_entry = txn.get(&field_key)?.ok_or_else(|| {
				Error(internal(format!(
					"RowSchema field {} missing for fingerprint {:?}",
					i, fingerprint
				)))
			})?;

			let name = schema_field::SCHEMA.get_utf8(&field_entry.row, schema_field::NAME).to_string();
			let base_type = schema_field::SCHEMA.get_u8(&field_entry.row, schema_field::TYPE);

			let constraint_type =
				schema_field::SCHEMA.get_u8(&field_entry.row, schema_field::CONSTRAINT_TYPE);

			let constraint_param1 =
				schema_field::SCHEMA.get_u32(&field_entry.row, schema_field::CONSTRAINT_P1);

			let constraint_param2 =
				schema_field::SCHEMA.get_u32(&field_entry.row, schema_field::CONSTRAINT_P2);

			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = schema_field::SCHEMA.get_u32(&field_entry.row, schema_field::OFFSET);
			let size = schema_field::SCHEMA.get_u32(&field_entry.row, schema_field::SIZE);
			let align = schema_field::SCHEMA.get_u8(&field_entry.row, schema_field::ALIGN);

			fields.push(RowSchemaField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		let schema = RowSchema::from_parts(fingerprint, fields);
		Span::current().record("cache_hit", false);
		Span::current().record("field_count", schema.field_count());
		self.0.cache.insert(fingerprint, schema.clone());

		Ok(Some(schema))
	}

	/// Insert a schema into the cache (used by loader during startup).
	///
	/// This does NOT persist the schema - it assumes it already exists in storage.
	#[instrument(
		name = "row_schema_registry::cache_row_schema",
		level = "trace",
		skip(schema),
		fields(fingerprint = ?schema.fingerprint(), field_count = schema.field_count())
	)]
	pub(crate) fn cache_row_schema(&self, schema: RowSchema) {
		self.0.cache.insert(schema.fingerprint(), schema);
	}

	/// Get the number of cached schemas.
	pub fn cache_size(&self) -> usize {
		self.0.cache.len()
	}

	/// List all cached schemas.
	///
	/// Returns all schemas currently in the cache. Note that this only returns
	/// schemas that have been loaded or created during this session.
	pub fn list_all(&self) -> Vec<RowSchema> {
		self.0.cache.iter().map(|entry| entry.value().clone()).collect()
	}

	/// Clear the cache (useful for testing).
	#[cfg(test)]
	pub fn clear_cache(&self) {
		self.0.cache.clear();
	}
}

impl fmt::Debug for RowSchemaRegistry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RowSchemaRegistry").field("cache_size", &self.0.cache.len()).finish_non_exhaustive()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn test_schema_registry_caching() {
		let registry = RowSchemaRegistry::new(SingleTransaction::testing());

		let fields = vec![
			RowSchemaField::unconstrained("id", Type::Int8),
			RowSchemaField::unconstrained("name", Type::Utf8),
		];

		// Create schema and insert into cache manually for testing
		let schema = RowSchema::new(fields);
		registry.cache_row_schema(schema.clone());

		// Should find it in cache
		let cached = registry.get(schema.fingerprint());
		assert!(cached.is_some());
		assert_eq!(schema, cached.unwrap());

		// Cache should have one entry
		assert_eq!(registry.cache_size(), 1);
	}

	#[test]
	fn test_schema_registry_get() {
		let registry = RowSchemaRegistry::new(SingleTransaction::testing());

		let fields = vec![RowSchemaField::unconstrained("x", Type::Float8)];
		let schema = RowSchema::new(fields);
		let fingerprint = schema.fingerprint();

		registry.cache_row_schema(schema);

		// Should find it in cache
		assert!(registry.get(fingerprint).is_some());

		// Unknown fingerprint should return None
		assert!(registry.get(RowSchemaFingerprint::new(0xDEADBEEF)).is_none());
	}
}
