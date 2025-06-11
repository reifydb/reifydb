// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueKind;
use crate::row::{Layout, EncodedRow};

impl Layout {
    pub fn get_bool(&self, row: &EncodedRow, index: usize) -> bool {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Bool);
        unsafe { (row.as_ptr().add(field.offset) as *const bool).read_unaligned() }
    }

    pub fn get_f32(&self, row: &EncodedRow, index: usize) -> f32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Float4);
        unsafe { (row.as_ptr().add(field.offset) as *const f32).read_unaligned() }
    }

    pub fn get_f64(&self, row: &EncodedRow, index: usize) -> f64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Float8);
        unsafe { (row.as_ptr().add(field.offset) as *const f64).read_unaligned() }
    }

    pub fn get_i8(&self, row: &EncodedRow, index: usize) -> i8 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Int1);
        unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
    }

    pub fn get_i16(&self, row: &EncodedRow, index: usize) -> i16 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Int2);
        unsafe { (row.as_ptr().add(field.offset) as *const i16).read_unaligned() }
    }

    pub fn get_i32(&self, row: &EncodedRow, index: usize) -> i32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Int4);
        unsafe { (row.as_ptr().add(field.offset) as *const i32).read_unaligned() }
    }

    pub fn get_i64(&self, row: &EncodedRow, index: usize) -> i64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Int8);
        unsafe { (row.as_ptr().add(field.offset) as *const i64).read_unaligned() }
    }

    pub fn get_i128(&self, row: &EncodedRow, index: usize) -> i128 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Int16);
        unsafe { (row.as_ptr().add(field.offset) as *const i128).read_unaligned() }
    }

    pub fn get_str(&self, row: &EncodedRow, index: usize) -> &str {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::String);

        unsafe {
            let base = row.as_ptr().add(field.offset);
            let len = *base as usize;
            let data = base.add(1);
            let slice = std::slice::from_raw_parts(data, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    pub fn get_u8(&self, row: &EncodedRow, index: usize) -> u8 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Uint1);
        unsafe { row.as_ptr().add(field.offset).read_unaligned() }
    }

    pub fn get_u16(&self, row: &EncodedRow, index: usize) -> u16 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Uint2);
        unsafe { (row.as_ptr().add(field.offset) as *const u16).read_unaligned() }
    }

    pub fn get_u32(&self, row: &EncodedRow, index: usize) -> u32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Uint4);
        unsafe { (row.as_ptr().add(field.offset) as *const u32).read_unaligned() }
    }

    pub fn get_u64(&self, row: &EncodedRow, index: usize) -> u64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Uint8);
        unsafe { (row.as_ptr().add(field.offset) as *const u64).read_unaligned() }
    }

    pub fn get_u128(&self, row: &EncodedRow, index: usize) -> u128 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.data_size);
        debug_assert_eq!(field.value, ValueKind::Uint16);
        unsafe { (row.as_ptr().add(field.offset) as *const u128).read_unaligned() }
    }
}

#[cfg(test)]
mod tests {
    use crate::row::Layout;
    use crate::ValueKind;

    #[test]
    fn test_get_bool() {
        let layout = Layout::new(&[ValueKind::Bool]);
        let mut row = layout.allocate_row();
        layout.set_bool(&mut row, 0, true);
        assert_eq!(layout.get_bool(&row, 0), true);
    }

    #[test]
    fn test_get_f32() {
        let layout = Layout::new(&[ValueKind::Float4]);
        let mut row = layout.allocate_row();
        layout.set_f32(&mut row, 0, 1.25f32);
        assert_eq!(layout.get_f32(&row, 0), 1.25f32);
    }

    #[test]
    fn test_get_f64() {
        let layout = Layout::new(&[ValueKind::Float8]);
        let mut row = layout.allocate_row();
        layout.set_f64(&mut row, 0, 2.5f64);
        assert_eq!(layout.get_f64(&row, 0), 2.5f64);
    }

    #[test]
    fn test_get_i8() {
        let layout = Layout::new(&[ValueKind::Int1]);
        let mut row = layout.allocate_row();
        layout.set_i8(&mut row, 0, 42i8);
        assert_eq!(layout.get_i8(&row, 0), 42i8);
    }

    #[test]
    fn test_get_i16() {
        let layout = Layout::new(&[ValueKind::Int2]);
        let mut row = layout.allocate_row();
        layout.set_i16(&mut row, 0, -1234i16);
        assert_eq!(layout.get_i16(&row, 0), -1234i16);
    }

    #[test]
    fn test_get_i32() {
        let layout = Layout::new(&[ValueKind::Int4]);
        let mut row = layout.allocate_row();
        layout.set_i32(&mut row, 0, 56789i32);
        assert_eq!(layout.get_i32(&row, 0), 56789i32);
    }

    #[test]
    fn test_get_i64() {
        let layout = Layout::new(&[ValueKind::Int8]);
        let mut row = layout.allocate_row();
        layout.set_i64(&mut row, 0, -987654321i64);
        assert_eq!(layout.get_i64(&row, 0), -987654321i64);
    }

    #[test]
    fn test_get_i128() {
        let layout = Layout::new(&[ValueKind::Int16]);
        let mut row = layout.allocate_row();
        layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
        assert_eq!(layout.get_i128(&row, 0), 123456789012345678901234567890i128);
    }

    #[test]
    fn test_get_str() {
        let layout = Layout::new(&[ValueKind::String]);
        let mut row = layout.allocate_row();
        layout.set_str(&mut row, 0, "reifydb");
        assert_eq!(layout.get_str(&row, 0), "reifydb");
    }

    #[test]
    fn test_get_u8() {
        let layout = Layout::new(&[ValueKind::Uint1]);
        let mut row = layout.allocate_row();
        layout.set_u8(&mut row, 0, 255u8);
        assert_eq!(layout.get_u8(&row, 0), 255u8);
    }

    #[test]
    fn test_get_u16() {
        let layout = Layout::new(&[ValueKind::Uint2]);
        let mut row = layout.allocate_row();
        layout.set_u16(&mut row, 0, 65535u16);
        assert_eq!(layout.get_u16(&row, 0), 65535u16);
    }

    #[test]
    fn test_get_u32() {
        let layout = Layout::new(&[ValueKind::Uint4]);
        let mut row = layout.allocate_row();
        layout.set_u32(&mut row, 0, 4294967295u32);
        assert_eq!(layout.get_u32(&row, 0), 4294967295u32);
    }

    #[test]
    fn test_get_u64() {
        let layout = Layout::new(&[ValueKind::Uint8]);
        let mut row = layout.allocate_row();
        layout.set_u64(&mut row, 0, 18446744073709551615u64);
        assert_eq!(layout.get_u64(&row, 0), 18446744073709551615u64);
    }

    #[test]
    fn test_get_u128() {
        let layout = Layout::new(&[ValueKind::Uint16]);
        let mut row = layout.allocate_row();
        layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
        assert_eq!(layout.get_u128(&row, 0), 340282366920938463463374607431768211455u128);
    }
}
