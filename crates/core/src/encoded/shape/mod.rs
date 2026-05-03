// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Row-shape descriptor: the schema-of-bytes that explains how to interpret an `EncodedRow`.
//!
//! A `RowShape` lists every field (name, type constraint, byte offset, byte size, alignment) so storage backends,
//! replication, and CDC can address fields without consulting the catalog. Submodules cover shape consolidation across
//! rows of the same logical schema, schema evolution rules for adding and removing fields, structural fingerprinting
//! used by plan caches and migration tooling, and conversion routines from typed schemas.
//!
//! Invariant: the `SHAPE_HEADER_SIZE` constant and the packed-mode bit layout (mode bit, length mask, offset mask) are
//! part of the wire format. Reordering or resizing any of these regions silently breaks every row written under the
//! previous layout.

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
	sync::{Arc, LazyLock, OnceLock},
};

use reifydb_type::{
	util::cowvec::CowVec,
	value::{constraint::TypeConstraint, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::row::EncodedRow;
use crate::encoded::shape::fingerprint::{RowShapeFingerprint, compute_fingerprint};

pub const SHAPE_HEADER_SIZE: usize = 24;

const PACKED_MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const PACKED_MODE_MASK: u128 = 0x80000000000000000000000000000000;
const PACKED_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const PACKED_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowShapeField {
	pub name: String,

	pub constraint: TypeConstraint,

	pub offset: u32,

	pub size: u32,

	pub align: u8,
}

impl RowShapeField {
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

	pub fn unconstrained(name: impl Into<String>, field_type: Type) -> Self {
		Self::new(name, TypeConstraint::unconstrained(field_type))
	}
}

pub struct RowShape(Arc<Inner>);

#[derive(Debug, Serialize, Deserialize)]
pub struct Inner {
	pub fingerprint: RowShapeFingerprint,

	pub fields: Vec<RowShapeField>,

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
	pub fn new(fields: Vec<RowShapeField>) -> Self {
		let fields = Self::compute_layout(fields);
		let fingerprint = compute_fingerprint(&fields);

		Self(Arc::new(Inner {
			fingerprint,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	pub fn from_parts(fingerprint: RowShapeFingerprint, fields: Vec<RowShapeField>) -> Self {
		Self(Arc::new(Inner {
			fingerprint,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	pub fn fingerprint(&self) -> RowShapeFingerprint {
		self.fingerprint
	}

	pub fn fields(&self) -> &[RowShapeField] {
		&self.fields
	}

	pub fn field_count(&self) -> usize {
		self.fields.len()
	}

	pub fn find_field(&self, name: &str) -> Option<&RowShapeField> {
		self.fields.iter().find(|f| f.name == name)
	}

	pub fn find_field_index(&self, name: &str) -> Option<usize> {
		self.fields.iter().position(|f| f.name == name)
	}

	pub fn get_field(&self, index: usize) -> Option<&RowShapeField> {
		self.fields.get(index)
	}

	pub fn get_field_name(&self, index: usize) -> Option<&str> {
		self.fields.get(index).map(|f| f.name.as_str())
	}

	pub fn field_names(&self) -> impl Iterator<Item = &str> {
		self.fields.iter().map(|f| f.name.as_str())
	}

	fn compute_layout(mut fields: Vec<RowShapeField>) -> Vec<RowShapeField> {
		let bitvec_size = fields.len().div_ceil(8);
		let mut offset: u32 = (SHAPE_HEADER_SIZE + bitvec_size) as u32;

		for field in fields.iter_mut() {
			let storage_type = field.constraint.storage_type();
			field.size = storage_type.size() as u32;
			field.align = storage_type.alignment() as u8;

			let align = field.align as u32;
			if align > 0 {
				offset = (offset + align - 1) & !(align - 1);
			}

			field.offset = offset;
			offset += field.size;
		}

		fields
	}

	pub fn bitvec_size(&self) -> usize {
		self.fields.len().div_ceil(8)
	}

	pub fn data_offset(&self) -> usize {
		SHAPE_HEADER_SIZE + self.bitvec_size()
	}

	fn get_cached_layout(&self) -> (usize, usize) {
		*self.cached_layout.get_or_init(|| {
			let max_align = self.fields.iter().map(|f| f.align as usize).max().unwrap_or(1);

			let total_size = if self.fields.is_empty() {
				SHAPE_HEADER_SIZE + self.bitvec_size()
			} else {
				let last_field = &self.fields[self.fields.len() - 1];
				let end = last_field.offset as usize + last_field.size as usize;

				Self::align_up(end, max_align)
			};

			(total_size, max_align)
		})
	}

	pub fn total_static_size(&self) -> usize {
		self.get_cached_layout().0
	}

	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	pub fn dynamic_section_size(&self, row: &EncodedRow) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

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
			Type::Int | Type::Uint | Type::Decimal => {
				let packed = unsafe {
					(row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned()
				};
				let packed = u128::from_le(packed);
				if packed & PACKED_MODE_MASK != 0 {
					let offset = (packed & PACKED_OFFSET_MASK) as usize;
					let length = ((packed & PACKED_LENGTH_MASK) >> 64) as usize;
					Some((offset, length))
				} else {
					None
				}
			}
			_ => None,
		}
	}

	pub(crate) fn write_dynamic_ref(&self, row: &mut EncodedRow, index: usize, offset: usize, length: usize) {
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			Type::Utf8 | Type::Blob | Type::Any => {
				let ref_slice = &mut row.0.make_mut()[field.offset as usize..field.offset as usize + 8];
				ref_slice[0..4].copy_from_slice(&(offset as u32).to_le_bytes());
				ref_slice[4..8].copy_from_slice(&(length as u32).to_le_bytes());
			}
			Type::Int | Type::Uint | Type::Decimal => {
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

	pub(crate) fn replace_dynamic_data(&self, row: &mut EncodedRow, index: usize, new_data: &[u8]) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(row, index) {
			let delta = new_data.len() as isize - old_length as isize;

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

			let dynamic_start = self.dynamic_section_start();
			let abs_start = dynamic_start + old_offset;
			let abs_end = abs_start + old_length;
			row.0.make_mut().splice(abs_start..abs_end, new_data.iter().copied());

			self.write_dynamic_ref(row, index, old_offset, new_data.len());

			for (i, off, len) in refs_to_update {
				let new_off = (off as isize + delta) as usize;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		} else {
			let dynamic_offset = self.dynamic_section_size(row);
			row.0.extend_from_slice(new_data);
			self.write_dynamic_ref(row, index, dynamic_offset, new_data.len());
		}
		row.set_valid(index, true);
	}

	pub(crate) fn remove_dynamic_data(&self, row: &mut EncodedRow, index: usize) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(row, index) {
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

			let dynamic_start = self.dynamic_section_start();
			let abs_start = dynamic_start + old_offset;
			let abs_end = abs_start + old_length;
			row.0.make_mut().splice(abs_start..abs_end, iter::empty());

			for (i, off, len) in refs_to_update {
				let new_off = off - old_length;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		}
	}

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

	pub fn set_none(&self, row: &mut EncodedRow, index: usize) {
		self.remove_dynamic_data(row, index);
		row.set_valid(index, false);
	}

	pub fn testing(types: &[Type]) -> Self {
		RowShape::new(
			types.iter()
				.enumerate()
				.map(|(i, t)| RowShapeField::unconstrained(format!("f{}", i), t.clone()))
				.collect(),
		)
	}

	pub fn operator_state() -> Self {
		OPERATOR_STATE_SHAPE.clone()
	}
}

static OPERATOR_STATE_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(vec![RowShapeField::unconstrained("state", Type::Blob)]));

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
