// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc};

use reifydb_type::Type;

use crate::{CowVec, row::EncodedRow};

#[derive(Debug, Clone)]
pub struct EncodedRowLayout(Arc<EncodedRowLayoutInner>);

impl Deref for EncodedRowLayout {
	type Target = EncodedRowLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedRowLayout {
	pub fn new(kinds: &[Type]) -> Self {
		Self(Arc::new(EncodedRowLayoutInner::new(kinds)))
	}
}

#[derive(Debug)]
pub struct EncodedRowLayoutInner {
	pub fields: Vec<Field>,
	/// size of data in bytes
	pub static_section_size: usize,
	/// size of bitvec part in bytes
	pub bitvec_size: usize,
	pub alignment: usize,
}

#[derive(Debug)]
pub struct Field {
	pub offset: usize,
	pub size: usize,
	pub align: usize,
	pub value: Type,
}

impl EncodedRowLayoutInner {
	fn new(kinds: &[Type]) -> Self {
		assert!(!kinds.is_empty());

		let num_fields = kinds.len();
		let bitvec_bytes = (num_fields + 7) / 8;

		let mut offset = bitvec_bytes;
		let mut fields = Vec::with_capacity(num_fields);
		let mut max_align = 1;

		for &value in kinds {
			let size = value.size();
			let align = value.alignment();

			offset = align_up(offset, align);
			fields.push(Field {
				offset,
				size,
				align,
				value,
			});

			offset += size;
			max_align = max_align.max(align);
		}

		// Calculate the static section size
		let static_section_size = align_up(offset, max_align);

		EncodedRowLayoutInner {
			fields,
			static_section_size,
			alignment: max_align,
			bitvec_size: bitvec_bytes,
		}
	}

	pub fn allocate_row(&self) -> EncodedRow {
		let total_size = self.total_static_size();
		let layout = std::alloc::Layout::from_size_align(
			total_size,
			self.alignment,
		)
		.unwrap();
		unsafe {
			let ptr = std::alloc::alloc_zeroed(layout);
			if ptr.is_null() {
				std::alloc::handle_alloc_error(layout);
			}
			// Safe because alloc_zeroed + known size/capacity
			let vec = Vec::from_raw_parts(
				ptr, total_size, total_size,
			);
			EncodedRow(CowVec::new(vec))
		}
	}

	pub const fn data_offset(&self) -> usize {
		self.bitvec_size
	}

	pub const fn static_section_size(&self) -> usize {
		self.static_section_size
	}

	pub const fn total_static_size(&self) -> usize {
		self.bitvec_size + self.static_section_size
	}

	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	pub fn dynamic_section_size(&self, row: &EncodedRow) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	pub fn data_slice<'a>(&'a self, row: &'a EncodedRow) -> &'a [u8] {
		&row.0[self.data_offset()..]
	}

	pub fn data_slice_mut<'a>(
		&'a mut self,
		row: &'a mut EncodedRow,
	) -> &'a mut [u8] {
		&mut row.0.make_mut()[self.data_offset()..]
	}

	pub fn all_defined(&self, row: &EncodedRow) -> bool {
		let bits = self.fields.len();
		if bits == 0 {
			return false;
		}

		let bitvec_slice = &row[..self.bitvec_size];
		for (i, &byte) in bitvec_slice.iter().enumerate() {
			let bits_in_byte =
				if i == self.bitvec_size - 1 && bits % 8 != 0 {
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
		self.fields[index].value
	}
}

fn align_up(offset: usize, align: usize) -> usize {
	(offset + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
	mod new {
		use reifydb_type::Type;

		use crate::row::EncodedRowLayout;

		#[test]
		fn test_single_field_bool() {
			let layout = EncodedRowLayout::new(&[Type::Boolean]);
			assert_eq!(layout.bitvec_size, 1);
			assert_eq!(layout.fields.len(), 1);
			assert_eq!(layout.fields[0].offset, 1);
			assert_eq!(layout.alignment, 1);
			assert_eq!(
				layout.static_section_size,
				layout.fields[0].offset + layout.fields[0].size
			);
		}

		#[test]
		fn test_multiple_fields() {
			let layout = EncodedRowLayout::new(&[
				Type::Int1,
				Type::Int2,
				Type::Int4,
			]);
			assert_eq!(layout.bitvec_size, 1); // 3 fields = 1 byte
			assert_eq!(layout.fields.len(), 3);

			assert_eq!(layout.fields[0].value, Type::Int1);
			assert_eq!(layout.fields[1].value, Type::Int2);
			assert_eq!(layout.fields[2].value, Type::Int4);

			assert_eq!(layout.fields[0].offset, 1);
			assert_eq!(layout.fields[1].offset, 2);
			assert_eq!(layout.fields[2].offset, 4);

			assert_eq!(layout.alignment, 4);

			assert_eq!(layout.static_section_size, 8); // 1 + 2 + 4 + 1(alignment)
		}

		#[test]
		fn test_offset_and_alignment() {
			let layout = EncodedRowLayout::new(&[
				Type::Uint1,
				Type::Uint2,
				Type::Uint4,
				Type::Uint8,
				Type::Uint16,
			]);

			assert_eq!(layout.bitvec_size, 1); // 5 fields = 1 byte
			assert_eq!(layout.fields.len(), 5);

			assert_eq!(layout.fields[0].offset, 1); // 1. byte is for bitvec
			assert_eq!(layout.fields[1].offset, 2);
			assert_eq!(layout.fields[2].offset, 4);
			assert_eq!(layout.fields[3].offset, 8);
			assert_eq!(layout.fields[4].offset, 16);

			assert_eq!(layout.alignment, 16);

			assert_eq!(layout.static_section_size, 32); // 1 + 2 + 4 + 8 + 16 + 1 (alignment)
		}

		#[test]
		fn test_nine_fields_bitvec_size_two() {
			let kinds = vec![
				Type::Boolean,
				Type::Int1,
				Type::Int2,
				Type::Int4,
				Type::Int8,
				Type::Uint1,
				Type::Uint2,
				Type::Uint4,
				Type::Uint8,
			];

			let layout = EncodedRowLayout::new(&kinds);

			// 9 fields → ceil(9/8) = 2 bytes of bitvec bitmap
			assert_eq!(layout.bitvec_size, 2);
			assert_eq!(layout.fields.len(), 9);

			assert_eq!(layout.fields[0].offset, 2); // first 2 bytes are for bitvec

			// All field offsets must come after the 2 bitvec bytes
			for field in &layout.fields {
				assert!(field.offset >= 2);
				assert_eq!(field.offset % field.align, 0);
			}

			assert_eq!(
				layout.static_section_size % layout.alignment,
				0
			);
		}
	}

	mod allocate_row {
		use reifydb_type::Type;

		use crate::row::EncodedRowLayout;

		#[test]
		fn test_initial_state() {
			let layout = EncodedRowLayout::new(&[
				Type::Boolean,
				Type::Int1,
				Type::Uint2,
			]);

			let row = layout.allocate_row();

			for byte in row.as_slice() {
				assert_eq!(*byte, 0);
			}

			assert_eq!(row.len(), layout.total_static_size());
		}

		#[test]
		fn test_clone_on_write_semantics() {
			let layout = EncodedRowLayout::new(&[
				Type::Boolean,
				Type::Boolean,
				Type::Boolean,
			]);

			let row1 = layout.allocate_row();
			let mut row2 = row1.clone();

			// Initially identical
			assert_eq!(row1.as_slice(), row2.as_slice());

			// Modify one row's bitvec bit
			row2.set_valid(1, true);

			// Internal buffers must now differ
			assert_ne!(row1.as_ptr(), row2.as_ptr());

			// row1 remains unchanged
			assert!(!row1.is_defined(1));
			// row2 has been mutated
			assert!(row2.is_defined(1));
		}
	}

	mod all_defined {
		use reifydb_type::Type;

		use crate::row::EncodedRowLayout;

		#[test]
		fn test_one_field_none_valid() {
			let layout = EncodedRowLayout::new(&[Type::Boolean; 1]);
			let mut row = layout.allocate_row();
			layout.set_undefined(&mut row, 0);
			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_one_field_valid() {
			let layout = EncodedRowLayout::new(&[Type::Boolean; 1]);
			let mut row = layout.allocate_row();
			layout.set_bool(&mut row, 0, true);
			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_none_valid() {
			let kinds = vec![Type::Boolean; 7];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..7 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_allv() {
			let kinds = vec![Type::Boolean; 7];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..7 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_partial_valid() {
			let kinds = vec![Type::Boolean; 7];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..7 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			for idx in [0, 3] {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_eight_fields_none_valid() {
			let kinds = vec![Type::Boolean; 8];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..8 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_eight_fields_allv() {
			let kinds = vec![Type::Boolean; 8];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..8 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_eight_fields_partial_valid() {
			let kinds = vec![Type::Boolean; 8];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..8 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			for idx in [0, 3, 7] {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_nine_fields_allv() {
			let kinds = vec![Type::Boolean; 9];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..9 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_nine_fields_none_valid() {
			let kinds = vec![Type::Boolean; 9];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..9 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_nine_fields_partial_valid() {
			let kinds = vec![Type::Boolean; 9];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..9 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			for idx in [0, 3, 7] {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_sixteen_fields_allv() {
			let kinds = vec![Type::Boolean; 16];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..16 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_sixteen_fields_none_valid() {
			let kinds = vec![Type::Boolean; 16];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..16 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_sixteen_fields_partial_valid() {
			let kinds = vec![Type::Boolean; 16];
			let layout = EncodedRowLayout::new(&kinds);
			let mut row = layout.allocate_row();

			for idx in 0..16 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			for idx in [0, 3, 7] {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}
	}
}
