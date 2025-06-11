// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueKind;
use crate::row::Layout;

impl Layout {
    pub fn get_bool(&self, row: &[u8], index: usize) -> bool {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Bool);
        unsafe { (row.as_ptr().add(field.offset) as *const bool).read_unaligned() }
    }

    pub fn get_f32(&self, row: &[u8], index: usize) -> f32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Float4);
        unsafe { (row.as_ptr().add(field.offset) as *const f32).read_unaligned() }
    }

    pub fn get_f64(&self, row: &[u8], index: usize) -> f64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Float8);
        unsafe { (row.as_ptr().add(field.offset) as *const f64).read_unaligned() }
    }

    pub fn get_i8(&self, row: &[u8], index: usize) -> i8 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int1);
        unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
    }

    pub fn get_i16(&self, row: &[u8], index: usize) -> i16 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int2);
        unsafe { (row.as_ptr().add(field.offset) as *const i16).read_unaligned() }
    }

    pub fn get_i32(&self, row: &[u8], index: usize) -> i32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int4);
        unsafe { (row.as_ptr().add(field.offset) as *const i32).read_unaligned() }
    }

    pub fn get_i64(&self, row: &[u8], index: usize) -> i64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int8);
        unsafe { (row.as_ptr().add(field.offset) as *const i64).read_unaligned() }
    }

    pub fn get_i128(&self, row: &[u8], index: usize) -> i128 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int16);
        unsafe { (row.as_ptr().add(field.offset) as *const i128).read_unaligned() }
    }

    pub fn get_str(&self, row: &[u8], index: usize) -> &str {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::String);

        unsafe {
            let base = row.as_ptr().add(field.offset);
            let len = *base as usize;
            let data = base.add(1);
            let slice = std::slice::from_raw_parts(data, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    pub fn get_u8(&self, row: &[u8], index: usize) -> u8 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint1);
        unsafe { (row.as_ptr().add(field.offset) as *const u8).read_unaligned() }
    }

    pub fn get_u16(&self, row: &[u8], index: usize) -> u16 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint2);
        unsafe { (row.as_ptr().add(field.offset) as *const u16).read_unaligned() }
    }

    pub fn get_u32(&self, row: &[u8], index: usize) -> u32 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint4);
        unsafe { (row.as_ptr().add(field.offset) as *const u32).read_unaligned() }
    }

    pub fn get_u64(&self, row: &[u8], index: usize) -> u64 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint8);
        unsafe { (row.as_ptr().add(field.offset) as *const u64).read_unaligned() }
    }

    pub fn get_u128(&self, row: &[u8], index: usize) -> u128 {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint16);
        unsafe { (row.as_ptr().add(field.offset) as *const u128).read_unaligned() }
    }
}
