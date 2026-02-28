// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_type::{Result, util::cowvec::CowVec, value::r#type::Type};

use crate::{error::CoreError, sort::SortDirection, value::index::encoded::EncodedIndexKey};

#[derive(Debug, Clone)]
pub struct EncodedIndexLayout(Arc<EncodedIndexLayoutInner>);

impl Deref for EncodedIndexLayout {
	type Target = EncodedIndexLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedIndexLayout {
	pub fn new(types: &[Type], directions: &[SortDirection]) -> Result<Self> {
		if types.len() != directions.len() {
			return Err(CoreError::IndexTypesDirectionsMismatch {
				types_len: types.len(),
				directions_len: directions.len(),
			}
			.into());
		}

		for typ in types {
			if matches!(typ, Type::Utf8 | Type::Blob) {
				return Err(CoreError::IndexVariableLengthNotSupported.into());
			}
		}

		Ok(Self(Arc::new(EncodedIndexLayoutInner::new(types, directions))))
	}
}

#[derive(Debug)]
pub struct EncodedIndexLayoutInner {
	pub fields: Vec<IndexField>,
	pub total_size: usize,
	pub bitvec_size: usize,
	pub alignment: usize,
}

#[derive(Debug)]
pub struct IndexField {
	pub offset: usize,
	pub size: usize,
	pub align: usize,
	pub value: Type,
	pub direction: SortDirection,
}

impl EncodedIndexLayoutInner {
	fn new(types: &[Type], directions: &[SortDirection]) -> Self {
		assert!(!types.is_empty());
		assert_eq!(types.len(), directions.len());

		let num_fields = types.len();
		let bitvec_bytes = (num_fields + 7) / 8;

		let mut offset = bitvec_bytes;
		let mut fields = Vec::with_capacity(num_fields);
		let mut max_align = 1;

		for (i, value) in types.iter().enumerate() {
			let size = value.size();
			let align = value.alignment();

			offset = align_up(offset, align);
			fields.push(IndexField {
				offset,
				size,
				align,
				value: value.clone(),
				direction: directions[i].clone(),
			});

			offset += size;
			max_align = max_align.max(align);
		}

		let total_size = align_up(offset, max_align);

		EncodedIndexLayoutInner {
			fields,
			total_size,
			alignment: max_align,
			bitvec_size: bitvec_bytes,
		}
	}

	pub fn allocate_key(&self) -> EncodedIndexKey {
		let layout = std::alloc::Layout::from_size_align(self.total_size, self.alignment).unwrap();
		unsafe {
			let ptr = std::alloc::alloc_zeroed(layout);
			if ptr.is_null() {
				std::alloc::handle_alloc_error(layout);
			}
			let vec = Vec::from_raw_parts(ptr, self.total_size, self.total_size);
			EncodedIndexKey(CowVec::new(vec))
		}
	}

	pub const fn data_offset(&self) -> usize {
		self.bitvec_size
	}

	pub fn all_defined(&self, key: &EncodedIndexKey) -> bool {
		let bits = self.fields.len();
		if bits == 0 {
			return false;
		}

		let bitvec_slice = &key[..self.bitvec_size];
		for (i, &byte) in bitvec_slice.iter().enumerate() {
			let bits_in_byte = if i == self.bitvec_size - 1 && bits % 8 != 0 {
				bits % 8
			} else {
				8
			};

			let mask = if bits_in_byte == 8 {
				0xFF
			} else {
				(1u8 << bits_in_byte) - 1
			};
			if (byte & mask) != mask {
				return false;
			}
		}

		true
	}

	pub fn value(&self, index: usize) -> Type {
		self.fields[index].value.clone()
	}

	pub fn direction(&self, index: usize) -> &SortDirection {
		&self.fields[index].direction
	}
}

fn align_up(offset: usize, align: usize) -> usize {
	(offset + align - 1) & !(align - 1)
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::sort::SortDirection;

	#[test]
	fn test_single_field_int() {
		let layout = EncodedIndexLayout::new(&[Type::Int4], &[SortDirection::Asc]).unwrap();

		assert_eq!(layout.bitvec_size, 1);
		assert_eq!(layout.fields.len(), 1);
		assert_eq!(layout.fields[0].offset, 4); // aligned to 4 bytes
		assert_eq!(layout.fields[0].value, Type::Int4);
		assert_eq!(layout.fields[0].direction, SortDirection::Asc);
		assert_eq!(layout.alignment, 4);
		assert_eq!(layout.total_size, 8); // 1 bitvec + 3 padding + 4 int
	}

	#[test]
	fn test_multiple_fields_mixed_directions() {
		let layout = EncodedIndexLayout::new(
			&[Type::Int4, Type::Int8, Type::Uint8],
			&[SortDirection::Desc, SortDirection::Asc, SortDirection::Asc],
		)
		.unwrap();

		assert_eq!(layout.bitvec_size, 1);
		assert_eq!(layout.fields.len(), 3);

		assert_eq!(layout.fields[0].value, Type::Int4);
		assert_eq!(layout.fields[0].direction, SortDirection::Desc);

		assert_eq!(layout.fields[1].value, Type::Int8);
		assert_eq!(layout.fields[1].direction, SortDirection::Asc);

		assert_eq!(layout.fields[2].value, Type::Uint8);
		assert_eq!(layout.fields[2].direction, SortDirection::Asc);

		assert_eq!(layout.alignment, 8);
	}

	#[test]
	fn test_reject_variable_length_types() {
		let result =
			EncodedIndexLayout::new(&[Type::Int4, Type::Utf8], &[SortDirection::Asc, SortDirection::Asc]);
		assert!(result.is_err());

		let result = EncodedIndexLayout::new(&[Type::Blob], &[SortDirection::Desc]);
		assert!(result.is_err());
	}

	#[test]
	fn test_allocate_key() {
		let layout = EncodedIndexLayout::new(
			&[Type::Boolean, Type::Int4],
			&[SortDirection::Asc, SortDirection::Desc],
		)
		.unwrap();

		let key = layout.allocate_key();
		assert_eq!(key.len(), layout.total_size);

		for byte in key.as_slice() {
			assert_eq!(*byte, 0);
		}
	}
}
