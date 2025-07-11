// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::row::EncodedRow;
use crate::{CowVec, DataType};

#[derive(Debug)]
pub struct Field {
    pub offset: usize,
    pub size: usize,
    pub align: usize,
    pub value: DataType,
}

#[derive(Debug)]
pub struct Layout {
    pub fields: Vec<Field>,
    /// size of data in bytes
    pub data_size: usize,
    /// size of validity part in bytes
    pub validity_size: usize,
    pub alignment: usize,
}

impl Layout {
    pub fn new(kinds: &[DataType]) -> Self {
        assert!(!kinds.is_empty());

        let num_fields = kinds.len();
        let validity_bytes = (num_fields + 7) / 8;

        let mut offset = validity_bytes;
        let mut fields = Vec::with_capacity(num_fields);
        let mut max_align = 1;

        for &value in kinds {
            let size = value.size();
            let align = value.alignment();

            offset = align_up(offset, align);
            fields.push(Field { offset, size, align, value });

            offset += size;
            max_align = max_align.max(align);
        }

        let size = align_up(offset, max_align);
        Layout { fields, data_size: size, alignment: max_align, validity_size: validity_bytes }
    }

    pub fn allocate_row(&self) -> EncodedRow {
        let layout = std::alloc::Layout::from_size_align(self.data_size, self.alignment).unwrap();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Safe because alloc_zeroed + known size/capacity
            let vec = Vec::from_raw_parts(ptr, self.data_size, self.data_size);
            EncodedRow(CowVec::new(vec))
        }
    }

    pub const fn data_offset(&self) -> usize {
        self.validity_size
    }

    pub const fn total_size(&self) -> usize {
        self.data_size + self.validity_size
    }

    pub fn data_slice<'a>(&'a self, row: &'a EncodedRow) -> &'a [u8] {
        &row.0[self.data_offset()..]
    }

    pub fn data_slice_mut<'a>(&'a mut self, row: &'a mut EncodedRow) -> &'a mut [u8] {
        &mut row.0.make_mut()[self.data_offset()..]
    }

    pub fn all_defined(&self, row: &EncodedRow) -> bool {
        let bits = self.fields.len();
        if bits == 0 {
            return false;
        }

        let validity_slice = &row[..self.validity_size];
        for (i, &byte) in validity_slice.iter().enumerate() {
            let bits_in_byte =
                if i == self.validity_size - 1 && bits % 8 != 0 { bits % 8 } else { 8 };

            let mask = if bits_in_byte == 8 { 0xFF } else { (1u8 << bits_in_byte) - 1 };
            if (byte & mask) != mask {
                return false;
            }
        }

        true
    }

    pub fn value(&self, index: usize) -> DataType {
        self.fields[index].value
    }
}

fn align_up(offset: usize, align: usize) -> usize {
    (offset + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    mod new {
        use crate::DataType;
        use crate::row::Layout;

        #[test]
        fn test_single_field_bool() {
            let layout = Layout::new(&[DataType::Bool]);
            assert_eq!(layout.validity_size, 1);
            assert_eq!(layout.fields.len(), 1);
            assert_eq!(layout.fields[0].offset, 1);
            assert_eq!(layout.alignment, 1);
            assert_eq!(layout.data_size, layout.fields[0].offset + layout.fields[0].size);
        }

        #[test]
        fn test_multiple_fields() {
            let layout = Layout::new(&[DataType::Int1, DataType::Int2, DataType::Int4]);
            assert_eq!(layout.validity_size, 1); // 3 fields = 1 byte
            assert_eq!(layout.fields.len(), 3);

            assert_eq!(layout.fields[0].value, DataType::Int1);
            assert_eq!(layout.fields[1].value, DataType::Int2);
            assert_eq!(layout.fields[2].value, DataType::Int4);

            assert_eq!(layout.fields[0].offset, 1);
            assert_eq!(layout.fields[1].offset, 2);
            assert_eq!(layout.fields[2].offset, 4);

            assert_eq!(layout.alignment, 4);

            assert_eq!(layout.data_size, 8); // 1 + 2 + 4 + 1(alignment)
        }

        #[test]
        fn test_offset_and_alignment() {
            let layout = Layout::new(&[
                DataType::Uint1,
                DataType::Uint2,
                DataType::Uint4,
                DataType::Uint8,
                DataType::Uint16,
            ]);

            assert_eq!(layout.validity_size, 1); // 5 fields = 1 byte
            assert_eq!(layout.fields.len(), 5);

            assert_eq!(layout.fields[0].offset, 1); // 1. byte is for validity
            assert_eq!(layout.fields[1].offset, 2);
            assert_eq!(layout.fields[2].offset, 4);
            assert_eq!(layout.fields[3].offset, 8);
            assert_eq!(layout.fields[4].offset, 16);

            assert_eq!(layout.alignment, 16);

            assert_eq!(layout.data_size, 32); // 1 + 2 + 4 + 8 + 16 + 1 (alignment)
        }

        #[test]
        fn test_nine_fields_validity_size_two() {
            let kinds = vec![
				DataType::Bool,
				DataType::Int1,
				DataType::Int2,
				DataType::Int4,
				DataType::Int8,
				DataType::Uint1,
				DataType::Uint2,
				DataType::Uint4,
				DataType::Uint8,
            ];

            let layout = Layout::new(&kinds);

            // 9 fields → ceil(9/8) = 2 bytes of validity bitmap
            assert_eq!(layout.validity_size, 2);
            assert_eq!(layout.fields.len(), 9);

            assert_eq!(layout.fields[0].offset, 2); // first 2 bytes are for validity

            // All field offsets must come after the 2 validity bytes
            for field in &layout.fields {
                assert!(field.offset >= 2);
                assert_eq!(field.offset % field.align, 0);
            }

            assert_eq!(layout.data_size % layout.alignment, 0);
        }
    }

    mod allocate_row {
        use crate::DataType;
        use crate::row::Layout;

        #[test]
        fn test_initial_state() {
            let layout = Layout::new(&[DataType::Bool, DataType::Int1, DataType::Uint2]);

            let row = layout.allocate_row();

            for byte in row.as_slice() {
                assert_eq!(*byte, 0);
            }

            assert_eq!(row.len(), layout.data_size);
        }

        #[test]
        fn test_clone_on_write_semantics() {
            let layout = Layout::new(&[DataType::Bool, DataType::Bool, DataType::Bool]);

            let row1 = layout.allocate_row();
            let mut row2 = row1.clone();

            // Initially identical
            assert_eq!(row1.as_slice(), row2.as_slice());

            // Modify one row's validity bit
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
        use crate::DataType;
        use crate::row::Layout;

        #[test]
        fn test_one_field_none_valid() {
            let layout = Layout::new(&[DataType::Bool; 1]);
            let mut row = layout.allocate_row();
            layout.set_undefined(&mut row, 0);
            assert!(!layout.all_defined(&row));
        }

        #[test]
        fn test_one_field_valid() {
            let layout = Layout::new(&[DataType::Bool; 1]);
            let mut row = layout.allocate_row();
            layout.set_bool(&mut row, 0, true);
            assert!(layout.all_defined(&row));
        }

        #[test]
        fn test_seven_fields_none_valid() {
            let kinds = vec![DataType::Bool; 7];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..7 {
                layout.set_undefined(&mut row, idx);
            }

            assert!(!layout.all_defined(&row));
        }

        #[test]
        fn test_seven_fields_allv() {
            let kinds = vec![DataType::Bool; 7];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..7 {
                layout.set_bool(&mut row, idx, idx % 2 == 0);
            }

            assert!(layout.all_defined(&row));
        }

        #[test]
        fn test_seven_fields_partialv() {
            let kinds = vec![DataType::Bool; 7];
            let layout = Layout::new(&kinds);
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
            let kinds = vec![DataType::Bool; 8];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..8 {
                layout.set_undefined(&mut row, idx);
            }

            assert!(!layout.all_defined(&row));
        }

        #[test]
        fn test_eight_fields_allv() {
            let kinds = vec![DataType::Bool; 8];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..8 {
                layout.set_bool(&mut row, idx, idx % 2 == 0);
            }

            assert!(layout.all_defined(&row));
        }

        #[test]
        fn test_eight_fields_partialv() {
            let kinds = vec![DataType::Bool; 8];
            let layout = Layout::new(&kinds);
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
            let kinds = vec![DataType::Bool; 9];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..9 {
                layout.set_bool(&mut row, idx, idx % 2 == 0);
            }

            assert!(layout.all_defined(&row));
        }

        #[test]
        fn test_nine_fields_none_valid() {
            let kinds = vec![DataType::Bool; 9];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..9 {
                layout.set_undefined(&mut row, idx);
            }

            assert!(!layout.all_defined(&row));
        }

        #[test]
        fn test_nine_fields_partialv() {
            let kinds = vec![DataType::Bool; 9];
            let layout = Layout::new(&kinds);
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
            let kinds = vec![DataType::Bool; 16];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..16 {
                layout.set_bool(&mut row, idx, idx % 2 == 0);
            }

            assert!(layout.all_defined(&row));
        }

        #[test]
        fn test_sixteen_fields_none_valid() {
            let kinds = vec![DataType::Bool; 16];
            let layout = Layout::new(&kinds);
            let mut row = layout.allocate_row();

            for idx in 0..16 {
                layout.set_undefined(&mut row, idx);
            }

            assert!(!layout.all_defined(&row));
        }

        #[test]
        fn test_sixteen_fields_partialv() {
            let kinds = vec![DataType::Bool; 16];
            let layout = Layout::new(&kinds);
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
