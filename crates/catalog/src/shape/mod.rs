// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape Registry for content-addressable shape storage.
//!
//! The RowShapeRegistry provides:
//! - In-memory caching of shapes by fingerprint
//! - Thread-safe access for concurrent reads
//! - Single-writer semantics for creates

pub mod decode;
pub mod load;

use std::{fmt, sync::Arc};

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
	error::diagnostic::internal::internal,
	key::shape::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::{single::SingleTransaction, transaction::Transaction};
use reifydb_type::{
	error::Error,
	value::constraint::{FFITypeConstraint, TypeConstraint},
};
use tracing::{Span, field, instrument};

use crate::{
	Result,
	store::row_shape::{
		create::create_row_shape,
		find::find_row_shape_by_fingerprint,
		shape::{shape_field, shape_header},
	},
};

/// Thread-safe shape registry with content-addressable caching.
///
/// Shapes are stored by their fingerprint (a hash of their field definitions).
/// The same field configuration always produces the same fingerprint, enabling
/// deduplication of identical shapes.
#[derive(Clone)]
pub struct RowShapeRegistry(Arc<RowShapeRegistryInner>);

struct RowShapeRegistryInner {
	single: SingleTransaction,
	/// Cache of shapes by fingerprint
	cache: SkipMap<RowShapeFingerprint, RowShape>,
}

/// Compute all storage keys for a shape.
///
/// Single-version transactions require upfront key declaration for lock ordering.
/// This computes the header key and all field keys for a given shape.
#[instrument(
	name = "row_shape_registry::compute_keys",
	level = "trace",
	skip_all,
	fields(fingerprint = ?fingerprint, key_count = field_count + 1)
)]
fn compute_shape_keys(fingerprint: RowShapeFingerprint, field_count: usize) -> Vec<EncodedKey> {
	let mut keys = Vec::with_capacity(1 + field_count);

	// RowShape header key
	keys.push(RowShapeKey::encoded(fingerprint));

	// RowShape field keys
	for idx in 0..field_count {
		keys.push(RowShapeFieldKey::encoded(fingerprint, idx as u16));
	}

	keys
}

impl RowShapeRegistry {
	/// Create a new empty shape registry.
	pub fn new(single: SingleTransaction) -> Self {
		Self(Arc::new(RowShapeRegistryInner {
			single,
			cache: SkipMap::new(),
		}))
	}

	pub fn testing() -> Self {
		Self::new(SingleTransaction::testing())
	}

	/// Get an existing shape by fingerprint, or create and persist a new one.
	///
	/// This method is thread-safe with the following guarantees:
	/// - Cache reads are lock-free (via SkipMap)
	/// - Creates are serialized via write_lock
	/// - Double-check pattern prevents duplicate creates
	#[instrument(
		name = "row_shape_registry::get_or_create",
		level = "debug",
		skip(fields),
		fields(fingerprint = field::Empty, field_count = fields.len())
	)]
	pub fn get_or_create(&self, fields: Vec<RowShapeField>) -> Result<RowShape> {
		let shape = RowShape::new(fields);
		let fingerprint = shape.fingerprint();
		Span::current().record("fingerprint", field::debug(&fingerprint));

		// Fast path
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		// Double-check after acquiring lock
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			return Ok(entry.value().clone());
		}

		let keys = compute_shape_keys(fingerprint, shape.field_count());

		let mut cmd = self.0.single.begin_command(&keys)?;

		if let Some(stored_shape) = find_row_shape_by_fingerprint(&mut cmd, fingerprint)? {
			self.0.cache.insert(fingerprint, stored_shape.clone());
			// No commit needed for read-only path, just drop transaction
			return Ok(stored_shape);
		}

		create_row_shape(&mut cmd, &shape)?;

		cmd.commit()?;

		self.0.cache.insert(fingerprint, shape.clone());

		Ok(shape)
	}

	/// Look up a shape by fingerprint (cache only).
	///
	/// Returns None if the shape is not in the cache.
	#[instrument(
		name = "row_shape_registry::get",
		level = "trace",
		fields(fingerprint = ?fingerprint)
	)]
	pub fn get(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.0.cache.get(&fingerprint).map(|entry| entry.value().clone())
	}

	/// Look up a shape by fingerprint, checking storage if not cached.
	///
	/// This method accepts an external transaction for reading shapes.
	/// For creating new shapes, use `get_or_create()` instead.
	#[instrument(
		name = "row_shape_registry::get_or_load",
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
		fingerprint: RowShapeFingerprint,
		txn: &mut Transaction<'_>,
	) -> Result<Option<RowShape>> {
		// Check cache first
		if let Some(entry) = self.0.cache.get(&fingerprint) {
			let shape = entry.value().clone();
			Span::current().record("cache_hit", true);
			Span::current().record("field_count", shape.field_count());
			return Ok(Some(shape));
		}

		// Read shape header
		let header_key = RowShapeKey::encoded(fingerprint);
		let header_entry = match txn.get(&header_key)? {
			Some(entry) => entry,
			None => {
				Span::current().record("cache_hit", false);
				Span::current().record("field_count", 0);
				return Ok(None);
			}
		};

		let field_count = shape_header::SHAPE.get_u16(&header_entry.row, shape_header::FIELD_COUNT) as usize;

		let mut fields = Vec::with_capacity(field_count);
		for i in 0..field_count {
			let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
			let field_entry = txn.get(&field_key)?.ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.row, shape_field::NAME).to_string();
			let base_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::TYPE);

			let constraint_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::CONSTRAINT_TYPE);

			let constraint_param1 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P1);

			let constraint_param2 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P2);

			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::OFFSET);
			let size = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::SIZE);
			let align = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::ALIGN);

			fields.push(RowShapeField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		let shape = RowShape::from_parts(fingerprint, fields);
		Span::current().record("cache_hit", false);
		Span::current().record("field_count", shape.field_count());
		self.0.cache.insert(fingerprint, shape.clone());

		Ok(Some(shape))
	}

	/// Insert a shape into the cache (used by loader during startup).
	///
	/// This does NOT persist the shape - it assumes it already exists in storage.
	#[instrument(
		name = "row_shape_registry::cache_row_shape",
		level = "trace",
		skip(shape),
		fields(fingerprint = ?shape.fingerprint(), field_count = shape.field_count())
	)]
	pub(crate) fn cache_row_shape(&self, shape: RowShape) {
		self.0.cache.insert(shape.fingerprint(), shape);
	}

	/// Get the number of cached shapes.
	pub fn cache_size(&self) -> usize {
		self.0.cache.len()
	}

	/// List all cached shapes.
	///
	/// Returns all shapes currently in the cache. Note that this only returns
	/// shapes that have been loaded or created during this session.
	pub fn list_all(&self) -> Vec<RowShape> {
		self.0.cache.iter().map(|entry| entry.value().clone()).collect()
	}

	/// Clear the cache (useful for testing).
	#[cfg(test)]
	pub fn clear_cache(&self) {
		self.0.cache.clear();
	}
}

impl fmt::Debug for RowShapeRegistry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RowShapeRegistry").field("cache_size", &self.0.cache.len()).finish_non_exhaustive()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn test_shape_registry_caching() {
		let registry = RowShapeRegistry::new(SingleTransaction::testing());

		let fields = vec![
			RowShapeField::unconstrained("id", Type::Int8),
			RowShapeField::unconstrained("name", Type::Utf8),
		];

		// Create shape and insert into cache manually for testing
		let shape = RowShape::new(fields);
		registry.cache_row_shape(shape.clone());

		// Should find it in cache
		let cached = registry.get(shape.fingerprint());
		assert!(cached.is_some());
		assert_eq!(shape, cached.unwrap());

		// Cache should have one entry
		assert_eq!(registry.cache_size(), 1);
	}

	#[test]
	fn test_shape_registry_get() {
		let registry = RowShapeRegistry::new(SingleTransaction::testing());

		let fields = vec![RowShapeField::unconstrained("x", Type::Float8)];
		let shape = RowShape::new(fields);
		let fingerprint = shape.fingerprint();

		registry.cache_row_shape(shape);

		// Should find it in cache
		assert!(registry.get(fingerprint).is_some());

		// Unknown fingerprint should return None
		assert!(registry.get(RowShapeFingerprint::new(0xDEADBEEF)).is_none());
	}
}
