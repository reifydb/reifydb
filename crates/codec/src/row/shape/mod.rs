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
	value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType},
};
use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::bytes::{
	CATALOG_HEADER_SIZE, EncodedRowBuilder, SHAPE_HEADER_SIZE, read_created_at, read_defined_at, read_storage_time,
	read_updated_at,
};
use crate::row::{
	catalog::EncodedCatalogRowBuilder,
	dictionary::DICTIONARY_HEADER_SIZE,
	operator::{
		EncodedOperatorRowBuilder, OPERATOR_HEADER_SIZE, read_time as read_operator_time,
		write_time as write_operator_time,
	},
	shape::fingerprint::{RowShapeFingerprint, compute_fingerprint},
};

const PACKED_MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const PACKED_MODE_MASK: u128 = 0x80000000000000000000000000000000;
const PACKED_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const PACKED_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, RkyvArchive, RkyvSerialize, RkyvDeserialize)]
pub enum RowFamily {
	Deprecated,
	Catalog,
	Operator,
	Dictionary,
}

impl RowFamily {
	pub const fn header_size(self) -> usize {
		match self {
			Self::Deprecated => SHAPE_HEADER_SIZE,
			Self::Catalog => CATALOG_HEADER_SIZE,
			Self::Operator => OPERATOR_HEADER_SIZE,
			Self::Dictionary => DICTIONARY_HEADER_SIZE,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RkyvArchive, RkyvSerialize, RkyvDeserialize)]
pub struct RowShapeField {
	pub name: String,

	pub constraint: TypeConstraint,

	pub offset: u32,

	pub size: u32,
}

impl RowShapeField {
	pub fn new(name: impl Into<String>, constraint: TypeConstraint) -> Self {
		let storage_type = constraint.storage_type();
		Self {
			name: name.into(),
			constraint,
			offset: 0,
			size: storage_type.size() as u32,
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

	pub family: RowFamily,

	pub fields: Vec<RowShapeField>,

	#[serde(skip)]
	cached_layout: OnceLock<usize>,
}

impl PartialEq for Inner {
	fn eq(&self, other: &Self) -> bool {
		self.fingerprint == other.fingerprint && self.family == other.family && self.fields == other.fields
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
	pub fn new(family: RowFamily, fields: Vec<RowShapeField>) -> Self {
		let fields = Self::compute_layout(family, fields);
		let fingerprint = compute_fingerprint(&fields);

		Self(Arc::new(Inner {
			fingerprint,
			family,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	pub fn from_parts(family: RowFamily, fingerprint: RowShapeFingerprint, fields: Vec<RowShapeField>) -> Self {
		Self(Arc::new(Inner {
			fingerprint,
			family,
			fields,
			cached_layout: OnceLock::new(),
		}))
	}

	pub fn family(&self) -> RowFamily {
		self.family
	}

	pub fn header_size(&self) -> usize {
		self.family.header_size()
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

	fn compute_layout(family: RowFamily, mut fields: Vec<RowShapeField>) -> Vec<RowShapeField> {
		let bitvec_size = fields.len().div_ceil(8);
		let mut offset: u32 = (family.header_size() + bitvec_size) as u32;

		for field in fields.iter_mut() {
			field.size = field.constraint.storage_type().size() as u32;
			field.offset = offset;
			offset += field.size;
		}

		fields
	}

	pub fn bitvec_size(&self) -> usize {
		self.fields.len().div_ceil(8)
	}

	pub fn data_offset(&self) -> usize {
		self.header_size() + self.bitvec_size()
	}

	#[inline]
	pub fn is_defined(&self, row: &[u8], index: usize) -> bool {
		read_defined_at(row, self.header_size(), index)
	}

	#[inline]
	pub(crate) fn set_valid(&self, row: &mut EncodedRowBuilder, index: usize, valid: bool) {
		row.set_valid_at(self.header_size(), index, valid);
	}

	#[inline]
	pub fn time(&self, row: &[u8]) -> Option<DateTime> {
		match self.family {
			RowFamily::Operator => read_operator_time(row),
			_ => read_storage_time(row),
		}
	}

	#[inline]
	pub fn set_time(&self, row: &mut EncodedRowBuilder, time: DateTime) {
		match self.family {
			RowFamily::Operator => write_operator_time(row.as_mut_slice(), time),
			_ => row.set_time(time),
		}
	}

	#[inline]
	pub fn created_at(&self, row: &[u8]) -> DateTime {
		match self.family {
			RowFamily::Operator => panic!("operator rows carry no created_at"),
			_ => read_created_at(row),
		}
	}

	#[inline]
	pub fn updated_at(&self, row: &[u8]) -> DateTime {
		match self.family {
			RowFamily::Operator => panic!("operator rows carry no updated_at"),
			_ => read_updated_at(row),
		}
	}

	fn get_cached_layout(&self) -> usize {
		*self.cached_layout.get_or_init(|| match self.fields.last() {
			Some(last) => last.offset as usize + last.size as usize,
			None => self.header_size() + self.bitvec_size(),
		})
	}

	pub fn total_static_size(&self) -> usize {
		self.get_cached_layout()
	}

	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	pub fn dynamic_section_size(&self, row: &[u8]) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	pub(crate) fn read_dynamic_ref(&self, row: &[u8], index: usize) -> Option<(usize, usize)> {
		if !self.is_defined(row, index) {
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
					.filter(|(i, _)| *i != index && self.is_defined(row, *i))
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
		self.set_valid(row, index, true);
	}

	pub(crate) fn remove_dynamic_data(&self, row: &mut EncodedRowBuilder, index: usize) {
		if let Some((old_offset, old_length)) = self.read_dynamic_ref(&row[..], index) {
			let refs_to_update: Vec<(usize, usize, usize)> = self
				.fields()
				.iter()
				.enumerate()
				.filter(|(i, _)| *i != index && self.is_defined(row, *i))
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
		let total_size = self.get_cached_layout();
		let mut row = EncodedRowBuilder::zeroed(total_size);
		match self.family {
			RowFamily::Operator => write_operator_time(row.as_mut_slice(), DateTime::MAX),
			_ => row.set_fingerprint(self.fingerprint),
		}
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

	pub fn allocate_catalog(&self) -> EncodedCatalogRowBuilder {
		assert_eq!(self.family, RowFamily::Catalog, "allocate_catalog on a shape of another family");
		EncodedCatalogRowBuilder::wrap(self.allocate())
	}

	pub fn allocate_operator(&self) -> EncodedOperatorRowBuilder {
		assert_eq!(self.family, RowFamily::Operator, "allocate_operator on a shape of another family");
		EncodedOperatorRowBuilder::wrap(self.allocate())
	}

	pub fn set_none(&self, row: &mut EncodedRowBuilder, index: usize) {
		self.remove_dynamic_data(row, index);
		self.set_valid(row, index, false);
	}

	pub fn testing(types: &[ValueType]) -> Self {
		RowShape::new(
			RowFamily::Deprecated,
			types.iter()
				.enumerate()
				.map(|(i, t)| RowShapeField::unconstrained(format!("f{}", i), t.clone()))
				.collect(),
		)
	}
}
