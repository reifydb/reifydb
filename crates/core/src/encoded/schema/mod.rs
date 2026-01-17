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
mod from;

use std::{
	alloc::{Layout, alloc_zeroed, handle_alloc_error},
	fmt::Debug,
	ops::Deref,
	sync::Arc,
};

use reifydb_type::{
	util::cowvec::CowVec,
	value::{constraint::TypeConstraint, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::encoded::EncodedValues;
use crate::encoded::schema::fingerprint::{SchemaFingerprint, compute_fingerprint};

/// Size of schema header (fingerprint) in bytes
pub const SCHEMA_HEADER_SIZE: usize = 8;

/// A field within a schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
	/// Field name
	pub name: String,
	/// Field type constraint (includes base type and optional constraints like MaxBytes)
	pub constraint: TypeConstraint,
	/// Byte offset within the encoded row
	pub offset: u32,
	/// Size in bytes
	pub size: u32,
	/// Alignment requirement
	pub align: u8,
}

impl SchemaField {
	/// Create a new schema field with a type constraint.
	/// Offset, size, and alignment are computed when added to a Schema.
	pub fn new(name: impl Into<String>, constraint: TypeConstraint) -> Self {
		let base_type = constraint.get_type();
		Self {
			name: name.into(),
			constraint,
			offset: 0,
			size: base_type.size() as u32,
			align: base_type.alignment() as u8,
		}
	}

	/// Create a new schema field with an unconstrained type.
	/// Convenience method for the common case of no constraints.
	pub fn unconstrained(name: impl Into<String>, field_type: Type) -> Self {
		Self::new(name, TypeConstraint::unconstrained(field_type))
	}
}

/// A schema describing the structure of encoded row data.
pub struct Schema(Arc<Inner>);

/// Inner data for a schema describing the structure of encoded row data.
///
/// Schemas are immutable and content-addressable via their fingerprint.
/// The same field configuration always produces the same fingerprint,
/// enabling schema deduplication in the registry.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inner {
	/// Content-addressable fingerprint (hash of canonical field representation)
	pub fingerprint: SchemaFingerprint,
	/// Fields in definition order
	pub fields: Vec<SchemaField>,
}

impl Deref for Schema {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for Schema {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Debug for Schema {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

impl PartialEq for Schema {
	fn eq(&self, other: &Self) -> bool {
		self.0.as_ref() == other.0.as_ref()
	}
}

impl Eq for Schema {}

impl Schema {
	/// Create a new schema from a list of fields.
	///
	/// This computes the memory layout (offsets, alignment) and fingerprint.
	pub fn new(fields: Vec<SchemaField>) -> Self {
		let fields = Self::compute_layout(fields);
		let fingerprint = compute_fingerprint(&fields);

		Self(Arc::new(Inner {
			fingerprint,
			fields,
		}))
	}

	/// Create a schema from pre-computed fields and fingerprint.
	/// Used when loading from storage.
	pub fn from_parts(fingerprint: SchemaFingerprint, fields: Vec<SchemaField>) -> Self {
		Self(Arc::new(Inner {
			fingerprint,
			fields,
		}))
	}

	/// Get the schema's fingerprint
	pub fn fingerprint(&self) -> SchemaFingerprint {
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

	/// Find field index by name
	pub fn find_field_index(&self, name: &str) -> Option<usize> {
		self.fields.iter().position(|f| f.name == name)
	}

	/// Find a field by index
	pub fn get_field(&self, index: usize) -> Option<&SchemaField> {
		self.fields.get(index)
	}

	/// Get field name by index
	pub fn get_field_name(&self, index: usize) -> Option<&str> {
		self.fields.get(index).map(|f| f.name.as_str())
	}

	/// Get all field names as an iterator
	pub fn field_names(&self) -> impl Iterator<Item = &str> {
		self.fields.iter().map(|f| f.name.as_str())
	}

	/// Compute memory layout for fields.
	/// Returns the fields with computed offsets and the total row size.
	fn compute_layout(mut fields: Vec<SchemaField>) -> Vec<SchemaField> {
		// Start offset calculation from where data section begins (after header + bitvec)
		let bitvec_size = (fields.len() + 7) / 8;
		let mut offset: u32 = (SCHEMA_HEADER_SIZE + bitvec_size) as u32;

		for field in fields.iter_mut() {
			let base_type = field.constraint.get_type();
			field.size = base_type.size() as u32;
			field.align = base_type.alignment() as u8;

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

	/// Size of the bitvec section in bytes
	pub fn bitvec_size(&self) -> usize {
		(self.fields.len() + 7) / 8
	}

	/// Offset where field data starts (after header and bitvec)
	pub fn data_offset(&self) -> usize {
		SCHEMA_HEADER_SIZE + self.bitvec_size()
	}

	/// Total size of the static section
	pub fn total_static_size(&self) -> usize {
		if self.fields.is_empty() {
			return SCHEMA_HEADER_SIZE + self.bitvec_size();
		}
		let last_field = &self.fields[self.fields.len() - 1];
		let end = last_field.offset as usize + last_field.size as usize;
		// Align to maximum field alignment
		let max_align = self.fields.iter().map(|f| f.align as usize).max().unwrap_or(1);
		Self::align_up(end, max_align)
	}

	/// Start of the dynamic section
	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	/// Size of the dynamic section
	pub fn dynamic_section_size(&self, row: &EncodedValues) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	/// Allocate a new encoded row
	pub fn allocate(&self) -> EncodedValues {
		let total_size = self.total_static_size();
		let max_align = self.fields.iter().map(|f| f.align as usize).max().unwrap_or(1);
		let layout = Layout::from_size_align(total_size, max_align).unwrap();
		unsafe {
			let ptr = alloc_zeroed(layout);
			if ptr.is_null() {
				handle_alloc_error(layout);
			}
			let vec = Vec::from_raw_parts(ptr, total_size, total_size);
			let mut row = EncodedValues(CowVec::new(vec));
			row.set_fingerprint(self.fingerprint);
			row
		}
	}

	fn align_up(offset: usize, align: usize) -> usize {
		(offset + align).saturating_sub(1) & !(align.saturating_sub(1))
	}

	/// Set a field as undefined (not set)
	pub fn set_undefined(&self, row: &mut EncodedValues, index: usize) {
		row.set_valid(index, false);
	}

	/// Create a schema from a list of types.
	/// Fields are named f0, f1, f2, etc. and have unconstrained types.
	/// Useful for tests and simple state schemas.
	pub fn testing(types: &[Type]) -> Self {
		Schema::new(
			types.iter()
				.enumerate()
				.map(|(i, &t)| SchemaField::unconstrained(format!("f{}", i), t))
				.collect(),
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_schema_creation() {
		let fields = vec![
			SchemaField::unconstrained("id", Type::Int8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("active", Type::Boolean),
		];

		let schema = Schema::new(fields);

		assert_eq!(schema.field_count(), 3);
		assert_eq!(schema.fields()[0].name, "id");
		assert_eq!(schema.fields()[1].name, "name");
		assert_eq!(schema.fields()[2].name, "active");
	}

	#[test]
	fn test_schema_fingerprint_deterministic() {
		let fields1 =
			vec![SchemaField::unconstrained("a", Type::Int4), SchemaField::unconstrained("b", Type::Utf8)];

		let fields2 =
			vec![SchemaField::unconstrained("a", Type::Int4), SchemaField::unconstrained("b", Type::Utf8)];

		let schema1 = Schema::new(fields1);
		let schema2 = Schema::new(fields2);

		assert_eq!(schema1.fingerprint(), schema2.fingerprint());
	}

	#[test]
	fn test_schema_fingerprint_different_for_different_schemas() {
		let fields1 = vec![SchemaField::unconstrained("a", Type::Int4)];
		let fields2 = vec![SchemaField::unconstrained("a", Type::Int8)];

		let schema1 = Schema::new(fields1);
		let schema2 = Schema::new(fields2);

		assert_ne!(schema1.fingerprint(), schema2.fingerprint());
	}

	#[test]
	fn test_find_field() {
		let fields = vec![
			SchemaField::unconstrained("id", Type::Int8),
			SchemaField::unconstrained("name", Type::Utf8),
		];

		let schema = Schema::new(fields);

		assert!(schema.find_field("id").is_some());
		assert!(schema.find_field("name").is_some());
		assert!(schema.find_field("missing").is_none());
	}
}
