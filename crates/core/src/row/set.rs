// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DataType;
use crate::row::{EncodedRow, Layout};
use std::ptr;

impl Layout {
    pub fn set_bool(&self, row: &mut EncodedRow, index: usize, value: impl Into<bool>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Bool);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut bool,
                value.into(),
            )
        }
    }

    pub fn set_f32(&self, row: &mut EncodedRow, index: usize, value: impl Into<f32>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Float4);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut f32,
                value.into(),
            )
        }
    }

    pub fn set_f64(&self, row: &mut EncodedRow, index: usize, value: impl Into<f64>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Float8);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut f64,
                value.into(),
            )
        }
    }

    pub fn set_i8(&self, row: &mut EncodedRow, index: usize, value: impl Into<i8>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Int1);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut i8,
                value.into(),
            )
        }
    }

    pub fn set_i16(&self, row: &mut EncodedRow, index: usize, value: impl Into<i16>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Int2);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut i16,
                value.into(),
            )
        }
    }

    pub fn set_i32(&self, row: &mut EncodedRow, index: usize, value: impl Into<i32>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Int4);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut i32,
                value.into(),
            )
        }
    }

    pub fn set_i64(&self, row: &mut EncodedRow, index: usize, value: impl Into<i64>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Int8);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut i64,
                value.into(),
            )
        }
    }

    pub fn set_i128(&self, row: &mut EncodedRow, index: usize, value: impl Into<i128>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Int16);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut i128,
                value.into(),
            )
        }
    }

    pub fn set_str(&self, row: &mut EncodedRow, index: usize, value: impl AsRef<str>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Utf8);

        let bytes = value.as_ref().as_bytes();
        let len = bytes.len().min(254); // One byte for length
        row.set_valid(index, true);

        unsafe {
            let dst = row.make_mut().as_mut_ptr().add(field.offset);
            *dst = len as u8;
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst.add(1), len);
        }
    }

    pub fn set_u8(&self, row: &mut EncodedRow, index: usize, value: impl Into<u8>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Uint1);
        row.set_valid(index, true);
        unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset), value.into()) }
    }

    pub fn set_u16(&self, row: &mut EncodedRow, index: usize, value: impl Into<u16>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Uint2);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut u16,
                value.into(),
            )
        }
    }

    pub fn set_u32(&self, row: &mut EncodedRow, index: usize, value: impl Into<u32>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Uint4);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut u32,
                value.into(),
            )
        }
    }

    pub fn set_u64(&self, row: &mut EncodedRow, index: usize, value: impl Into<u64>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Uint8);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut u64,
                value.into(),
            )
        }
    }

    pub fn set_u128(&self, row: &mut EncodedRow, index: usize, value: impl Into<u128>) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, DataType::Uint16);
        row.set_valid(index, true);
        unsafe {
            ptr::write_unaligned(
                row.make_mut().as_mut_ptr().add(field.offset) as *mut u128,
                value.into(),
            )
        }
    }

    pub fn set_undefined(&self, row: &mut EncodedRow, index: usize) {
        debug_assert_eq!(row.len(), self.data_size);
        let field = &self.fields[index];

        row.set_valid(index, false);

        let buf = row.make_mut();
        let start = field.offset;
        let end = start + field.size;
        buf[start..end].fill(0);
    }
}

#[cfg(test)]
mod tests {
    use crate::DataType;
    use crate::row::Layout;

    #[test]
    fn test_bool_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Bool]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_bool(&mut row2, 0, true);

        assert!(row2.is_defined(0));

        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(raw[offset], 1u8);

        assert!(!row1.is_defined(0));
        assert_eq!(row1[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_f32_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Float4]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_f32(&mut row2, 0, 1.25f32);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + std::mem::size_of::<f32>()], &1.25f32.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_f64_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Float8]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_f64(&mut row2, 0, 3.5f64);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + std::mem::size_of::<f64>()], &3.5f64.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_i8_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int1]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_i8(&mut row2, 0, 42i8);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + 1], &42i8.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_i16_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int2]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_i16(&mut row2, 0, -1234i16);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + 2], &(-1234i16).to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_i32_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int4]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_i32(&mut row2, 0, 56789i32);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + 4], &56789i32.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_i64_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int8]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_i64(&mut row2, 0, 987654321i64);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + size_of::<i64>()], &987654321i64.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_i128_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int16]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_i128(&mut row2, 0, 123456789012345678901234567890i128);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(
            &raw[offset..offset + size_of::<i128>()],
            &123456789012345678901234567890i128.to_le_bytes()
        );

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_str_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Utf8]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_str(&mut row2, 0, "reifydb");

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        let len = raw[offset] as usize;
        let string_slice = std::str::from_utf8(&raw[offset + 1..offset + 1 + len]).unwrap();
        assert_eq!(string_slice, "reifydb");

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_u8_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Uint1]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_u8(&mut row2, 0, 255u8);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + std::mem::size_of::<u8>()], &255u8.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_u16_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Uint2]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_u16(&mut row2, 0, 65535u16);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + std::mem::size_of::<u16>()], &65535u16.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_u32_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Uint4]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_u32(&mut row2, 0, 4294967295u32);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(&raw[offset..offset + std::mem::size_of::<u32>()], &4294967295u32.to_le_bytes());

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_u64_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Uint8]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_u64(&mut row2, 0, 18446744073709551615u64);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(
            &raw[offset..offset + std::mem::size_of::<u64>()],
            &18446744073709551615u64.to_le_bytes()
        );

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_u128_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Uint16]);
        let row1 = layout.allocate_row();
        let mut row2 = row1.clone();

        assert!(!row1.is_defined(0));
        assert!(!row2.is_defined(0));

        layout.set_u128(&mut row2, 0, 340282366920938463463374607431768211455u128);

        assert!(row2.is_defined(0));
        let raw = &row2.0;
        let offset = layout.fields[0].offset;
        assert_eq!(
            &raw[offset..offset + std::mem::size_of::<u128>()],
            &340282366920938463463374607431768211455u128.to_le_bytes()
        );

        assert!(!row1.is_defined(0));
        assert_eq!(row1.0[offset], 0u8);
        assert_ne!(row1.as_ptr(), row2.as_ptr());
    }

    #[test]
    fn test_set_undefined_and_clone_on_write() {
        let layout = Layout::new(&[DataType::Int4]);
        let mut row1 = layout.allocate_row();

        layout.set_i32(&mut row1, 0, 12345);

        let mut row2 = row1.clone();
        assert!(row2.is_defined(0));

        layout.set_undefined(&mut row2, 0);
        assert!(!row2.is_defined(0));
        assert_eq!(layout.get_i32(&row2, 0), 0);

        assert!(row1.is_defined(0));
        assert_ne!(row1.as_ptr(), row2.as_ptr());
        assert_eq!(layout.get_i32(&row1, 0), 12345);
    }
}
