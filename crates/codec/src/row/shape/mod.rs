// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Row-shape descriptor: the schema-of-bytes that lets storage, replication and CDC address an
//! `EncodedBytes`'s fields without consulting the catalog. `SHAPE_HEADER_SIZE` and the packed-mode bit
//! layout are part of the wire format; resizing either breaks every row written under the old one.

pub mod cache;
pub mod fingerprint;
pub mod values;

use std::{
	fmt,
	fmt::Debug,
	iter,
	ops::Deref,
	ptr,
	sync::{Arc, OnceLock},
};

use reifydb_value::{
	reifydb_assertions,
	value::{constraint::TypeConstraint, value_type::ValueType},
};
use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::bytes::{EncodedRowBuilder, SHAPE_HEADER_SIZE, read_defined};
use crate::row::shape::fingerprint::{RowShapeFingerprint, compute_fingerprint};

const PACKED_MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const PACKED_MODE_MASK: u128 = 0x80000000000000000000000000000000;
const PACKED_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const PACKED_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RkyvArchive, RkyvSerialize, RkyvDeserialize)]
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

	pub fn unconstrained(name: impl Into<String>, field_type: ValueType) -> Self {
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

	pub fn dynamic_section_size(&self, row: &[u8]) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	pub(crate) fn read_dynamic_ref(&self, row: &[u8], index: usize) -> Option<(usize, usize)> {
		if !read_defined(row, index) {
			return None;
		}
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			ValueType::Utf8 | ValueType::Blob | ValueType::Any => {
				let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
				let offset =
					u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]])
						as usize;
				let length =
					u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]])
						as usize;
				Some((offset, length))
			}
			ValueType::Int | ValueType::Uint | ValueType::Decimal => {
				// SAFETY: these three types occupy a 16-byte static slot, and the shape
				// guarantees field.offset + 16 lies inside the row's static section;
				// read_unaligned needs no alignment and u128 has no invalid patterns.
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

	pub(crate) fn write_dynamic_ref(
		&self,
		row: &mut EncodedRowBuilder,
		index: usize,
		offset: usize,
		length: usize,
	) {
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			ValueType::Utf8 | ValueType::Blob | ValueType::Any => {
				let ref_slice =
					&mut row.as_mut_slice()[field.offset as usize..field.offset as usize + 8];
				ref_slice[0..4].copy_from_slice(&(offset as u32).to_le_bytes());
				ref_slice[4..8].copy_from_slice(&(length as u32).to_le_bytes());
			}
			ValueType::Int | ValueType::Uint | ValueType::Decimal => {
				let offset_part = (offset as u128) & PACKED_OFFSET_MASK;
				let length_part = ((length as u128) << 64) & PACKED_LENGTH_MASK;
				let packed = PACKED_MODE_DYNAMIC | offset_part | length_part;
				// SAFETY: these three types occupy a 16-byte static slot, and the shape
				// guarantees field.offset + 16 lies inside the row's static section;
				// make_mut() gives unique ownership and write_unaligned needs no alignment.
				unsafe {
					ptr::write_unaligned(
						row.as_mut_slice().as_mut_ptr().add(field.offset as usize) as *mut u128,
						packed.to_le(),
					);
				}
			}
			_ => {}
		}
	}

	pub(crate) fn replace_dynamic_data(&self, row: &mut EncodedRowBuilder, index: usize, new_data: &[u8]) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(&row[..], index) {
			let delta = new_data.len() as isize - old_length as isize;

			let refs_to_update: Vec<(usize, usize, usize)> = if delta != 0 {
				self.fields()
					.iter()
					.enumerate()
					.filter(|(i, _)| *i != index && read_defined(row, *i))
					.filter_map(|(i, _)| {
						self.read_dynamic_ref(&row[..], i)
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
			row.vec_mut().splice(abs_start..abs_end, new_data.iter().copied());

			self.write_dynamic_ref(row, index, old_offset, new_data.len());

			for (i, off, len) in refs_to_update {
				let new_off = (off as isize + delta) as usize;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		} else {
			let dynamic_offset = self.dynamic_section_size(&row[..]);
			row.extend_from_slice(new_data);
			self.write_dynamic_ref(row, index, dynamic_offset, new_data.len());
		}
		row.set_valid(index, true);
	}

	pub(crate) fn remove_dynamic_data(&self, row: &mut EncodedRowBuilder, index: usize) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(&row[..], index) {
			let refs_to_update: Vec<(usize, usize, usize)> = self
				.fields()
				.iter()
				.enumerate()
				.filter(|(i, _)| *i != index && read_defined(row, *i))
				.filter_map(|(i, _)| {
					self.read_dynamic_ref(&row[..], i)
						.filter(|(off, _)| *off > old_offset)
						.map(|(off, len)| (i, off, len))
				})
				.collect();

			let dynamic_start = self.dynamic_section_start();
			let abs_start = dynamic_start + old_offset;
			let abs_end = abs_start + old_length;
			row.vec_mut().splice(abs_start..abs_end, iter::empty());

			for (i, off, len) in refs_to_update {
				let new_off = off - old_length;
				self.write_dynamic_ref(row, i, new_off, len);
			}
		}
	}

	pub fn allocate(&self) -> EncodedRowBuilder {
		let (total_size, _) = self.get_cached_layout();
		let mut row = EncodedRowBuilder::zeroed(total_size);
		row.set_fingerprint(self.fingerprint);
		reifydb_assertions! {
			assert!(
				row.len() == total_size,
				"allocated row length does not match the shape total_static_size, so any field accessor using pre-computed offsets will read from garbage memory (row_len={} total_size={})",
				row.len(),
				total_size
			);
		}
		row
	}

	fn align_up(offset: usize, align: usize) -> usize {
		(offset + align).saturating_sub(1) & !(align.saturating_sub(1))
	}

	pub fn set_none(&self, row: &mut EncodedRowBuilder, index: usize) {
		self.remove_dynamic_data(row, index);
		row.set_valid(index, false);
	}

	pub fn testing(types: &[ValueType]) -> Self {
		RowShape::new(
			types.iter()
				.enumerate()
				.map(|(i, t)| RowShapeField::unconstrained(format!("f{}", i), t.clone()))
				.collect(),
		)
	}
}
