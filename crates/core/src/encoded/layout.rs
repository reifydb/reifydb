// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	alloc::{Layout, alloc_zeroed, handle_alloc_error},
	ops::Deref,
	sync::Arc,
};

use reifydb_type::{util::cowvec::CowVec, value::r#type::Type};

use super::{
	encoded::EncodedValues,
	schema::{Schema, SchemaFingerprint},
};

/// Size of schema header (fingerprint) in bytes
pub const SCHEMA_HEADER_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct EncodedValuesLayout(Arc<EncodedValuesLayoutInner>);

impl Deref for EncodedValuesLayout {
	type Target = EncodedValuesLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedValuesLayout {
	pub fn new(types: &[Type]) -> Self {
		Self(Arc::new(EncodedValuesLayoutInner::new(types)))
	}
}

impl From<&Schema> for EncodedValuesLayout {
	fn from(schema: &Schema) -> Self {
		let types: Vec<Type> = schema.fields().iter().map(|field| field.field_type).collect();

		EncodedValuesLayout::new(&types)
	}
}

#[derive(Debug)]
pub struct EncodedValuesLayoutInner {
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
	pub r#type: Type,
}

impl EncodedValuesLayoutInner {
	fn new(types: &[Type]) -> Self {
		assert!(!types.is_empty());

		let num_fields = types.len();
		let bitvec_bytes = (num_fields + 7) / 8;

		let mut offset = SCHEMA_HEADER_SIZE + bitvec_bytes;
		let mut fields = Vec::with_capacity(num_fields);
		let mut max_align = 1;

		for &value in types {
			let size = value.size();
			let align = value.alignment();

			offset = align_up(offset, align);
			fields.push(Field {
				offset,
				size,
				align,
				r#type: value,
			});

			offset += size;
			max_align = max_align.max(align);
		}

		// Calculate the static section size
		let static_section_size = align_up(offset, max_align);

		EncodedValuesLayoutInner {
			fields,
			static_section_size,
			alignment: max_align,
			bitvec_size: bitvec_bytes,
		}
	}

	/// Allocate a new row with the given schema fingerprint
	pub fn allocate(&self, fingerprint: SchemaFingerprint) -> EncodedValues {
		let total_size = self.total_static_size();
		let layout = Layout::from_size_align(total_size, self.alignment).unwrap();
		unsafe {
			let ptr = alloc_zeroed(layout);
			if ptr.is_null() {
				handle_alloc_error(layout);
			}
			// Safe because alloc_zeroed + known size/capacity
			let vec = Vec::from_raw_parts(ptr, total_size, total_size);
			let mut row = EncodedValues(CowVec::new(vec));
			row.set_fingerprint(fingerprint);
			row
		}
	}

	/// Allocate without fingerprint (for backwards compatibility during migration)
	#[deprecated(note = "Use allocate with SchemaFingerprint instead")]
	pub fn allocate_deprecated(&self) -> EncodedValues {
		self.allocate(SchemaFingerprint::zero())
	}

	pub fn allocate_for_testing(&self) -> EncodedValues {
		self.allocate(SchemaFingerprint::zero())
	}

	pub const fn data_offset(&self) -> usize {
		SCHEMA_HEADER_SIZE + self.bitvec_size
	}

	pub const fn static_section_size(&self) -> usize {
		self.static_section_size
	}

	pub const fn total_static_size(&self) -> usize {
		self.static_section_size
	}

	pub fn dynamic_section_start(&self) -> usize {
		self.total_static_size()
	}

	pub fn dynamic_section_size(&self, row: &EncodedValues) -> usize {
		row.len().saturating_sub(self.total_static_size())
	}

	pub fn data_slice<'a>(&'a self, row: &'a EncodedValues) -> &'a [u8] {
		&row.0[self.data_offset()..]
	}

	pub fn data_slice_mut<'a>(&'a mut self, row: &'a mut EncodedValues) -> &'a mut [u8] {
		&mut row.0.make_mut()[self.data_offset()..]
	}

	pub fn all_defined(&self, row: &EncodedValues) -> bool {
		let bits = self.fields.len();
		if bits == 0 {
			return false;
		}

		let bitvec_slice = &row[SCHEMA_HEADER_SIZE..SCHEMA_HEADER_SIZE + self.bitvec_size];
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
		self.fields[index].r#type
	}
}

fn align_up(offset: usize, align: usize) -> usize {
	(offset + align).saturating_sub(1) & !(align.saturating_sub(1))
}

#[cfg(test)]
pub mod tests {
	mod new {
		use reifydb_type::value::r#type::Type;

		use crate::encoded::layout::EncodedValuesLayout;

		#[test]
		fn test_single_field_bool() {
			let layout = EncodedValuesLayout::new(&[Type::Boolean]);
			assert_eq!(layout.bitvec_size, 1);
			assert_eq!(layout.fields.len(), 1);
			assert_eq!(layout.fields[0].offset, 9); // 8 (header) + 1 (bitvec)
			assert_eq!(layout.alignment, 1);
			assert_eq!(layout.total_static_size(), 8 + 1 + 1); // header + bitvec + data
		}

		#[test]
		fn test_multiple_fields() {
			let layout = EncodedValuesLayout::new(&[Type::Int1, Type::Int2, Type::Int4]);
			assert_eq!(layout.bitvec_size, 1); // 3 fields = 1 byte
			assert_eq!(layout.fields.len(), 3);

			assert_eq!(layout.fields[0].r#type, Type::Int1);
			assert_eq!(layout.fields[1].r#type, Type::Int2);
			assert_eq!(layout.fields[2].r#type, Type::Int4);

			assert_eq!(layout.fields[0].offset, 9); // 8 (header) + 1 (bitvec)
			assert_eq!(layout.fields[1].offset, 10); // 9 + 1, aligned to 2
			assert_eq!(layout.fields[2].offset, 12); // 10 + 2, aligned to 4

			assert_eq!(layout.alignment, 4);

			assert_eq!(layout.total_static_size(), 16); // 8 (header) + 1 (bitvec) + 1 + 2 + 4, aligned to 4
		}

		#[test]
		fn test_offset_and_alignment() {
			let layout = EncodedValuesLayout::new(&[
				Type::Uint1,
				Type::Uint2,
				Type::Uint4,
				Type::Uint8,
				Type::Uint16,
			]);

			assert_eq!(layout.bitvec_size, 1); // 5 fields = 1 byte
			assert_eq!(layout.fields.len(), 5);

			assert_eq!(layout.fields[0].offset, 9); // 8 (header) + 1 (bitvec)
			assert_eq!(layout.fields[1].offset, 10); // aligned to 2
			assert_eq!(layout.fields[2].offset, 12); // aligned to 4
			assert_eq!(layout.fields[3].offset, 16); // aligned to 8
			assert_eq!(layout.fields[4].offset, 32); // aligned to 16

			assert_eq!(layout.alignment, 16);

			assert_eq!(layout.total_static_size(), 48); // 32 + 16, aligned to 16
		}

		#[test]
		fn test_nine_fields_bitvec_size_two() {
			let types = vec![
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

			let layout = EncodedValuesLayout::new(&types);

			// 9 fields → ceil(9/8) = 2 bytes of bitvec bitmap
			assert_eq!(layout.bitvec_size, 2);
			assert_eq!(layout.fields.len(), 9);

			assert_eq!(layout.fields[0].offset, 10); // 8 (header) + 2 (bitvec)

			// All field offsets must come after the header + bitvec bytes
			for field in &layout.fields {
				assert!(field.offset >= 10); // 8 (header) + 2 (bitvec)
				assert_eq!(field.offset % field.align, 0);
			}

			assert_eq!(layout.total_static_size() % layout.alignment, 0);
		}
	}

	mod allocate_row {
		use reifydb_type::value::r#type::Type;

		use crate::encoded::layout::EncodedValuesLayout;

		#[test]
		fn test_initial_state() {
			let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Int1, Type::Uint2]);

			let row = layout.allocate_for_testing();

			for byte in row.as_slice() {
				assert_eq!(*byte, 0);
			}

			assert_eq!(row.len(), layout.total_static_size());
		}

		#[test]
		fn test_clone_on_write_semantics() {
			let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Boolean, Type::Boolean]);

			let row1 = layout.allocate_for_testing();
			let mut row2 = row1.clone();

			// Initially identical
			assert_eq!(row1.as_slice(), row2.as_slice());

			// Modify one encoded's bitvec bit
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
		use reifydb_type::value::r#type::Type;

		use crate::encoded::layout::EncodedValuesLayout;

		#[test]
		fn test_one_field_none_valid() {
			let layout = EncodedValuesLayout::new(&[Type::Boolean; 1]);
			let mut row = layout.allocate_for_testing();
			layout.set_undefined(&mut row, 0);
			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_one_field_valid() {
			let layout = EncodedValuesLayout::new(&[Type::Boolean; 1]);
			let mut row = layout.allocate_for_testing();
			layout.set_bool(&mut row, 0, true);
			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_none_valid() {
			let types = vec![Type::Boolean; 7];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..7 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_allv() {
			let types = vec![Type::Boolean; 7];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..7 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_seven_fields_partial_valid() {
			let types = vec![Type::Boolean; 7];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

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
			let types = vec![Type::Boolean; 8];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..8 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_eight_fields_allv() {
			let types = vec![Type::Boolean; 8];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..8 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_eight_fields_partial_valid() {
			let types = vec![Type::Boolean; 8];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

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
			let types = vec![Type::Boolean; 9];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..9 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_nine_fields_none_valid() {
			let types = vec![Type::Boolean; 9];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..9 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_nine_fields_partial_valid() {
			let types = vec![Type::Boolean; 9];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

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
			let types = vec![Type::Boolean; 16];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..16 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			assert!(layout.all_defined(&row));
		}

		#[test]
		fn test_sixteen_fields_none_valid() {
			let types = vec![Type::Boolean; 16];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..16 {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}

		#[test]
		fn test_sixteen_fields_partial_valid() {
			let types = vec![Type::Boolean; 16];
			let layout = EncodedValuesLayout::new(&types);
			let mut row = layout.allocate_for_testing();

			for idx in 0..16 {
				layout.set_bool(&mut row, idx, idx % 2 == 0);
			}

			for idx in [0, 3, 7] {
				layout.set_undefined(&mut row, idx);
			}

			assert!(!layout.all_defined(&row));
		}
	}

	mod from_schema {
		use reifydb_type::value::r#type::Type;

		use crate::encoded::{
			layout::EncodedValuesLayout,
			schema::{Schema, SchemaField},
		};

		#[test]
		fn test_from_schema_single_field() {
			let schema = Schema::new(vec![SchemaField::new("id", Type::Int8)]);

			let layout = EncodedValuesLayout::from(&schema);

			assert_eq!(layout.fields.len(), 1);
			assert_eq!(layout.fields[0].r#type, Type::Int8);
			assert_eq!(layout.bitvec_size, 1);
		}

		#[test]
		fn test_from_schema_multiple_fields_alignment() {
			let schema = Schema::new(vec![
				SchemaField::new("a", Type::Int1),
				SchemaField::new("b", Type::Int2),
				SchemaField::new("c", Type::Int4),
			]);

			let layout_from_schema = EncodedValuesLayout::from(&schema);
			let layout_direct = EncodedValuesLayout::new(&[Type::Int1, Type::Int2, Type::Int4]);

			// Verify offsets match direct construction
			assert_eq!(layout_from_schema.fields.len(), layout_direct.fields.len());
			for (from_schema, direct) in layout_from_schema.fields.iter().zip(layout_direct.fields.iter()) {
				assert_eq!(from_schema.offset, direct.offset);
				assert_eq!(from_schema.size, direct.size);
				assert_eq!(from_schema.align, direct.align);
				assert_eq!(from_schema.r#type, direct.r#type);
			}
			assert_eq!(layout_from_schema.alignment, layout_direct.alignment);
			assert_eq!(layout_from_schema.total_static_size(), layout_direct.total_static_size());
		}

		#[test]
		fn test_from_schema_nine_fields_bitvec_size() {
			let schema = Schema::new(vec![
				SchemaField::new("f0", Type::Boolean),
				SchemaField::new("f1", Type::Int1),
				SchemaField::new("f2", Type::Int2),
				SchemaField::new("f3", Type::Int4),
				SchemaField::new("f4", Type::Int8),
				SchemaField::new("f5", Type::Uint1),
				SchemaField::new("f6", Type::Uint2),
				SchemaField::new("f7", Type::Uint4),
				SchemaField::new("f8", Type::Uint8),
			]);

			let layout = EncodedValuesLayout::from(&schema);

			// 9 fields → bitvec grows to 2 bytes
			assert_eq!(layout.bitvec_size, 2);
			assert_eq!(layout.fields.len(), 9);
		}

		#[test]
		fn test_from_schema_preserves_field_order() {
			let schema = Schema::new(vec![
				SchemaField::new("first", Type::Utf8),
				SchemaField::new("second", Type::Int4),
				SchemaField::new("third", Type::Boolean),
			]);

			let layout = EncodedValuesLayout::from(&schema);

			assert_eq!(layout.fields[0].r#type, Type::Utf8);
			assert_eq!(layout.fields[1].r#type, Type::Int4);
			assert_eq!(layout.fields[2].r#type, Type::Boolean);
		}

		#[test]
		fn test_from_schema_equivalence_with_direct_construction() {
			let types = vec![Type::Uint1, Type::Uint2, Type::Uint4, Type::Uint8, Type::Uint16];

			let schema = Schema::new(
				types.iter()
					.enumerate()
					.map(|(i, t)| SchemaField::new(format!("f{}", i), *t))
					.collect(),
			);

			let layout_from_schema = EncodedValuesLayout::from(&schema);
			let layout_direct = EncodedValuesLayout::new(&types);

			// Full equivalence check
			assert_eq!(layout_from_schema.fields.len(), layout_direct.fields.len());
			assert_eq!(layout_from_schema.bitvec_size, layout_direct.bitvec_size);
			assert_eq!(layout_from_schema.alignment, layout_direct.alignment);
			assert_eq!(layout_from_schema.static_section_size, layout_direct.static_section_size);

			for (i, (from_schema, direct)) in
				layout_from_schema.fields.iter().zip(layout_direct.fields.iter()).enumerate()
			{
				assert_eq!(from_schema.offset, direct.offset, "offset mismatch at field {}", i);
				assert_eq!(from_schema.size, direct.size, "size mismatch at field {}", i);
				assert_eq!(from_schema.align, direct.align, "align mismatch at field {}", i);
				assert_eq!(from_schema.r#type, direct.r#type, "type mismatch at field {}", i);
			}
		}
	}
}
