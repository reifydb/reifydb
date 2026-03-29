// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape definitions for encoding row data with consistent field layouts.
//!
//! A `RowShape` describes the structure of encoded row data, including:
//! - Field names, types, and order
//! - Memory layout (offsets, sizes, alignment)
//! - A content-addressable fingerprint for deduplication

pub mod consolidate;
pub mod evolution;
pub mod fingerprint;
mod from;

use std::{
	alloc::{Layout, alloc_zeroed, handle_alloc_error},
	fmt,
	fmt::Debug,
	iter,
	ops::Deref,
	ptr,
	sync::{Arc, OnceLock},
};

use reifydb_type::{
	util::cowvec::CowVec,
	value::{constraint::TypeConstraint, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::row::EncodedRow;
use crate::encoded::shape::fingerprint::{RowShapeFingerprint, compute_fingerprint};

/// Size of shape header (fingerprint) in bytes
pub const SCHEMA_HEADER_SIZE: usize = 8;

/// Constants for packed u128 dynamic references (used by Int, Uint, Decimal)
const PACKED_MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const PACKED_MODE_MASK: u128 = 0x80000000000000000000000000000000;
const PACKED_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const PACKED_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

/// A field within a shape
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowShapeField {
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

impl RowShapeField {
	/// Create a new shape field with a type constraint.
	/// Offset, size, and alignment are computed when added to a RowShape.
	pub fn new(name: impl Into<String>, constraint: TypeConstraint) -> Self {
		let storage_type = constraint.storage_type();
		Self {
			name: name.into(),
			constraint,
			offset: 0,
			size: storage_type.size() as u32,
			align: storage_type.alignment() as u8,
		}
	}

	/// Create a new shape field with an unconstrained type.
	/// Convenience method for the common case of no constraints.
	pub fn unconstrained(name: impl Into<String>, field_type: Type) -> Self {
		Self::new(name, TypeConstraint::unconstrained(field_type))
	}
}

/// A shape describing the structure of encoded row data.
pub struct RowShape(Arc<Inner>);

/// Inner data for a shape describing the structure of encoded row data.
///
/// Shapes are immutable and content-addressable via their fingerprint.
/// The same field configuration always produces the same fingerprint,
/// enabling shape deduplication in the registry.
#[derive(Debug, Serialize, Deserialize)]
pub struct Inner {
	/// Content-addressable fingerprint (hash of canonical field representation)
	pub fingerprint: RowShapeFingerprint,
	/// Fields in definition order
	pub fields: Vec<RowShapeField>,
	/// Cached layout computation (total_size, max_align) - computed once on first use
	#[serde(skip)]
	cached_layout: OnceLock<(usize, usize)>,
}

impl PartialEq for Inner {
	fn eq(&self, other: &Self) -> bool {
		self.fingerprint == other.fingerprint && self.fields == other.fields
	}
}

impl Eq for Inner {}

impl Deref for RowShape {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for RowShape {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Debug for RowShape {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl PartialEq for RowShape {
	fn eq(&self, other: &Self) -> bool {
		self.0.as_ref() == other.0.as_ref()
	}
}

impl Eq for RowShape {}

impl RowShape {
	/// Create a new shape from a list of fields.
	///
	/// This computes the memory layout (offsets, alignment) and fingerprint.
	pub fn new(fields: Vec<RowShapeField>) -> Self {
		let fields = Self::compute_layout(fields);
		let fingerprint = compute_fingerprint(&fields);

		Self(Arc::new(Inner {
			fingerprint,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	/// Create a shape from pre-computed fields and fingerprint.
	/// Used when loading from storage.
	pub fn from_parts(fingerprint: RowShapeFingerprint, fields: Vec<RowShapeField>) -> Self {
		Self(Arc::new(Inner {
			fingerprint,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	/// Get the shape's fingerprint
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		self.fingerprint
	}

	/// Get the fields in this shape
	pub fn fields(&self) -> &[RowShapeField] {
		&self.fields
	}

	/// Get the number of fields
	pub fn field_count(&self) -> usize {
		self.fields.len()
	}

	/// Find a field by name
	pub fn find_field(&self, name: &str) -> Option<&RowShapeField> {
		self.fields.iter().find(|f| f.name == name)
	}

	/// Find field index by name
	pub fn find_field_index(&self, name: &str) -> Option<usize> {
		self.fields.iter().position(|f| f.name == name)
	}

	/// Find a field by index
	pub fn get_field(&self, index: usize) -> Option<&RowShapeField> {
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
	fn compute_layout(mut fields: Vec<RowShapeField>) -> Vec<RowShapeField> {
		// Start offset calculation from where data section begins (after header + bitvec)
		let bitvec_size = (fields.len() + 7) / 8;
		let mut offset: u32 = (SCHEMA_HEADER_SIZE + bitvec_size) as u32;

		for field in fields.iter_mut() {
			let storage_type = field.constraint.storage_type();
			field.size = storage_type.size() as u32;
			field.align = storage_type.alignment() as u8;

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

	/// Compute and cache the layout (total_size, max_align).
	/// This is called once and the result is cached for subsequent calls.
	fn get_cached_layout(&self) -> (usize, usize) {
		*self.cached_layout.get_or_init(|| {
			// Compute max_align
			let max_align = self.fields.iter().map(|f| f.align as usize).max().unwrap_or(1);

			// Compute total_size
			let total_size = if self.fields.is_empty() {
				SCHEMA_HEADER_SIZE + self.bitvec_size()
			} else {
				let last_field = &self.fields[self.fields.len() - 1];
				let end = last_field.offset as usize + last_field.size as usize;
				// Align to maximum field alignment
				Self::align_up(end, max_align)
			};

			(total_size, max_align)
		})
	}

	/// Total size of the static section
	pub fn total_static_size(&self) -> usize {
		self.get_cached_layout().0
	}

	/// Start of the dynamic section
	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	/// Size of the dynamic section
	pub fn dynamic_section_size(&self, row: &EncodedRow) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	/// Returns (offset, length) in the dynamic section for a defined dynamic field.
	/// Returns None if field is undefined, static-only, or uses inline storage.
	pub(crate) fn read_dynamic_ref(&self, row: &EncodedRow, index: usize) -> Option<(usize, usize)> {
		if !row.is_defined(index) {
			return None;
		}
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			Type::Utf8 | Type::Blob | Type::Any => {
				let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
				let offset =
					u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]])
						as usize;
				let length =
					u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]])
						as usize;
				Some((offset, length))
			}
			Type::Int
			| Type::Uint
			| Type::Decimal {
				..
			} => {
				let packed = unsafe {
					(row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned()
				};
				let packed = u128::from_le(packed);
				if packed & PACKED_MODE_MASK != 0 {
					let offset = (packed & PACKED_OFFSET_MASK) as usize;
					let length = ((packed & PACKED_LENGTH_MASK) >> 64) as usize;
					Some((offset, length))
				} else {
					None // inline storage
				}
			}
			_ => None,
		}
	}

	/// Writes a dynamic section reference for the given field in its type-appropriate format.
	pub(crate) fn write_dynamic_ref(&self, row: &mut EncodedRow, index: usize, offset: usize, length: usize) {
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			Type::Utf8 | Type::Blob | Type::Any => {
				let ref_slice = &mut row.0.make_mut()[field.offset as usize..field.offset as usize + 8];
				ref_slice[0..4].copy_from_slice(&(offset as u32).to_le_bytes());
				ref_slice[4..8].copy_from_slice(&(length as u32).to_le_bytes());
			}
			Type::Int
			| Type::Uint
			| Type::Decimal {
				..
			} => {
				let offset_part = (offset as u128) & PACKED_OFFSET_MASK;
				let length_part = ((length as u128) << 64) & PACKED_LENGTH_MASK;
				let packed = PACKED_MODE_DYNAMIC | offset_part | length_part;
				unsafe {
					ptr::write_unaligned(
						row.0.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u128,
						packed.to_le(),
					);
				}
			}
			_ => {}
		}
	}

	/// Replace dynamic data for a field. Handles both first-set (append) and update (splice).
	/// On update: splices old bytes out, inserts new bytes, adjusts all other dynamic refs.
	pub(crate) fn replace_dynamic_data(&self, row: &mut EncodedRow, index: usize, new_data: &[u8]) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(row, index) {
			let delta = new_data.len() as isize - old_length as isize;

			// Collect refs that need adjusting BEFORE splice
			let refs_to_update: Vec<(usize, usize, usize)> = if delta != 0 {
				self.fields()
					.iter()
					.enumerate()
					.filter(|(i, _)| *i != index && row.is_defined(*i))
					.filter_map(|(i, _)| {
						self.read_dynamic_ref(row, i)
							.filter(|(off, _)| *off > old_offset)
							.map(|(off, len)| (i, off, len))
					})
					.collect()
			} else {
				vec![]
			};

			// Splice bytes in the dynamic section
			let dynamic_start = self.dynamic_section_start();
			let abs_start = dynamic_start + old_offset;
			let abs_end = abs_start + old_length;
			row.0.make_mut().splice(abs_start..abs_end, new_data.iter().copied());

			// Update this field's reference (same offset, new length)
			self.write_dynamic_ref(row, index, old_offset, new_data.len());

			// Adjust other dynamic references by the size delta
			for (i, off, len) in refs_to_update {
				let new_off = (off as isize + delta) as usize;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		} else {
			// First set or transitioning from inline — append to dynamic section
			let dynamic_offset = self.dynamic_section_size(row);
			row.0.extend_from_slice(new_data);
			self.write_dynamic_ref(row, index, dynamic_offset, new_data.len());
		}
		row.set_valid(index, true);
	}

	/// Remove dynamic data for a field without setting new data.
	/// Used for dynamic→inline transitions in Int/Uint.
	pub(crate) fn remove_dynamic_data(&self, row: &mut EncodedRow, index: usize) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(row, index) {
			// Collect refs that need adjusting
			let refs_to_update: Vec<(usize, usize, usize)> = self
				.fields()
				.iter()
				.enumerate()
				.filter(|(i, _)| *i != index && row.is_defined(*i))
				.filter_map(|(i, _)| {
					self.read_dynamic_ref(row, i)
						.filter(|(off, _)| *off > old_offset)
						.map(|(off, len)| (i, off, len))
				})
				.collect();

			// Remove bytes
			let dynamic_start = self.dynamic_section_start();
			let abs_start = dynamic_start + old_offset;
			let abs_end = abs_start + old_length;
			row.0.make_mut().splice(abs_start..abs_end, iter::empty());

			// Adjust other references
			for (i, off, len) in refs_to_update {
				let new_off = off - old_length;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		}
	}

	/// Allocate a new encoded row
	pub fn allocate(&self) -> EncodedRow {
		let (total_size, max_align) = self.get_cached_layout();
		let layout = Layout::from_size_align(total_size, max_align).unwrap();
		unsafe {
			let ptr = alloc_zeroed(layout);
			if ptr.is_null() {
				handle_alloc_error(layout);
			}
			let vec = Vec::from_raw_parts(ptr, total_size, total_size);
			let mut row = EncodedRow(CowVec::new(vec));
			row.set_fingerprint(self.fingerprint);
			row
		}
	}

	fn align_up(offset: usize, align: usize) -> usize {
		(offset + align).saturating_sub(1) & !(align.saturating_sub(1))
	}

	/// Set a field as undefined (not set)
	pub fn set_none(&self, row: &mut EncodedRow, index: usize) {
		self.remove_dynamic_data(row, index);
		row.set_valid(index, false);
	}

	/// Create a shape from a list of types.
	/// Fields are named f0, f1, f2, etc. and have unconstrained types.
	/// Useful for tests and simple state shapes.
	pub fn testing(types: &[Type]) -> Self {
		RowShape::new(
			types.iter()
				.enumerate()
				.map(|(i, t)| RowShapeField::unconstrained(format!("f{}", i), t.clone()))
				.collect(),
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_shape_creation() {
		let fields = vec![
			RowShapeField::unconstrained("id", Type::Int8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("active", Type::Boolean),
		];

		let shape = RowShape::new(fields);

		assert_eq!(shape.field_count(), 3);
		assert_eq!(shape.fields()[0].name, "id");
		assert_eq!(shape.fields()[1].name, "name");
		assert_eq!(shape.fields()[2].name, "active");
	}

	#[test]
	fn test_shape_fingerprint_deterministic() {
		let fields1 = vec![
			RowShapeField::unconstrained("a", Type::Int4),
			RowShapeField::unconstrained("b", Type::Utf8),
		];

		let fields2 = vec![
			RowShapeField::unconstrained("a", Type::Int4),
			RowShapeField::unconstrained("b", Type::Utf8),
		];

		let shape1 = RowShape::new(fields1);
		let shape2 = RowShape::new(fields2);

		assert_eq!(shape1.fingerprint(), shape2.fingerprint());
	}

	#[test]
	fn test_shape_fingerprint_different_for_different_shapes() {
		let fields1 = vec![RowShapeField::unconstrained("a", Type::Int4)];
		let fields2 = vec![RowShapeField::unconstrained("a", Type::Int8)];

		let shape1 = RowShape::new(fields1);
		let shape2 = RowShape::new(fields2);

		assert_ne!(shape1.fingerprint(), shape2.fingerprint());
	}

	#[test]
	fn test_find_field() {
		let fields = vec![
			RowShapeField::unconstrained("id", Type::Int8),
			RowShapeField::unconstrained("name", Type::Utf8),
		];

		let shape = RowShape::new(fields);

		assert!(shape.find_field("id").is_some());
		assert!(shape.find_field("name").is_some());
		assert!(shape.find_field("missing").is_none());
	}
}
