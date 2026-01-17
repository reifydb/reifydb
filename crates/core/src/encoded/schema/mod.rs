// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema definitions for encoding row data with consistent field layouts.
//!
//! A `Schema` describes the structure of encoded row data, including:
//! - Field names, types, and order
//! - Memory layout (offsets, sizes, alignment)
//! - A content-addressable fingerprint for deduplication

pub mod consolidate;
pub mod evolution;
pub mod fingerprint;

use std::sync::Arc;

pub use fingerprint::compute_fingerprint;
use reifydb_hash::Hash64;
use reifydb_type::value::r#type::Type;
use serde::{Deserialize, Serialize};

/// A field within a schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
	/// Field name
	pub name: String,
	/// Field data type
	pub field_type: Type,
	/// Field index within the schema (0-based)
	pub field_index: u8,
	/// Byte offset within the encoded row
	pub offset: u32,
	/// Size in bytes
	pub size: u32,
	/// Alignment requirement
	pub align: u8,
}

impl SchemaField {
	/// Create a new schema field with minimal information.
	/// Offset, size, and alignment are computed when added to a Schema.
	pub fn new(name: impl Into<String>, field_type: Type) -> Self {
		Self {
			name: name.into(),
			field_type,
			field_index: 0,
			offset: 0,
			size: field_type.size() as u32,
			align: field_type.alignment() as u8,
		}
	}

	/// Builder method to set the field index
	pub fn with_index(mut self, index: u8) -> Self {
		self.field_index = index;
		self
	}
}

/// A schema describing the structure of encoded row data.
///
/// Schemas are immutable and content-addressable via their fingerprint.
/// The same field configuration always produces the same fingerprint,
/// enabling schema deduplication in the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
	/// Content-addressable fingerprint (hash of canonical field representation)
	fingerprint: Hash64,
	/// Fields in definition order
	fields: Vec<SchemaField>,
}

impl Schema {
	/// Create a new schema from a list of fields.
	///
	/// This computes the memory layout (offsets, alignment) and fingerprint.
	pub fn new(fields: Vec<SchemaField>) -> Self {
		let fields = Self::compute_layout(fields);
		let fingerprint = compute_fingerprint(&fields);

		Self {
			fingerprint,
			fields,
		}
	}

	/// Create a schema from pre-computed fields and fingerprint.
	/// Used when loading from storage.
	pub fn from_parts(fingerprint: Hash64, fields: Vec<SchemaField>) -> Self {
		Self {
			fingerprint,
			fields,
		}
	}

	/// Get the schema's fingerprint
	pub fn fingerprint(&self) -> Hash64 {
		self.fingerprint
	}

	/// Get the fields in this schema
	pub fn fields(&self) -> &[SchemaField] {
		&self.fields
	}

	/// Get the number of fields
	pub fn field_count(&self) -> usize {
		self.fields.len()
	}

	/// Find a field by name
	pub fn find_field(&self, name: &str) -> Option<&SchemaField> {
		self.fields.iter().find(|f| f.name == name)
	}

	/// Find a field by index
	pub fn get_field(&self, index: usize) -> Option<&SchemaField> {
		self.fields.get(index)
	}

	/// Compute memory layout for fields.
	/// Returns the fields with computed offsets and the total row size.
	fn compute_layout(mut fields: Vec<SchemaField>) -> Vec<SchemaField> {
		let mut offset: u32 = 0;

		for (idx, field) in fields.iter_mut().enumerate() {
			field.field_index = idx as u8;
			field.size = field.field_type.size() as u32;
			field.align = field.field_type.alignment() as u8;

			// Align offset
			let align = field.align as u32;
			if align > 0 {
				offset = (offset + align - 1) & !(align - 1);
			}

			field.offset = offset;
			offset += field.size;
		}

		fields
	}
}

/// Wrapper for thread-safe schema sharing
pub type SharedSchema = Arc<Schema>;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_schema_creation() {
		let fields = vec![
			SchemaField::new("id", Type::Int8),
			SchemaField::new("name", Type::Utf8),
			SchemaField::new("active", Type::Boolean),
		];

		let schema = Schema::new(fields);

		assert_eq!(schema.field_count(), 3);
		assert_eq!(schema.fields()[0].name, "id");
		assert_eq!(schema.fields()[1].name, "name");
		assert_eq!(schema.fields()[2].name, "active");

		// Check indices were assigned
		assert_eq!(schema.fields()[0].field_index, 0);
		assert_eq!(schema.fields()[1].field_index, 1);
		assert_eq!(schema.fields()[2].field_index, 2);
	}

	#[test]
	fn test_schema_fingerprint_deterministic() {
		let fields1 = vec![SchemaField::new("a", Type::Int4), SchemaField::new("b", Type::Utf8)];

		let fields2 = vec![SchemaField::new("a", Type::Int4), SchemaField::new("b", Type::Utf8)];

		let schema1 = Schema::new(fields1);
		let schema2 = Schema::new(fields2);

		assert_eq!(schema1.fingerprint(), schema2.fingerprint());
	}

	#[test]
	fn test_schema_fingerprint_different_for_different_schemas() {
		let fields1 = vec![SchemaField::new("a", Type::Int4)];
		let fields2 = vec![SchemaField::new("a", Type::Int8)];

		let schema1 = Schema::new(fields1);
		let schema2 = Schema::new(fields2);

		assert_ne!(schema1.fingerprint(), schema2.fingerprint());
	}

	#[test]
	fn test_find_field() {
		let fields = vec![SchemaField::new("id", Type::Int8), SchemaField::new("name", Type::Utf8)];

		let schema = Schema::new(fields);

		assert!(schema.find_field("id").is_some());
		assert!(schema.find_field("name").is_some());
		assert!(schema.find_field("missing").is_none());
	}
}
