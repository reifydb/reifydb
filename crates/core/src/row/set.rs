// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueKind;
use crate::row::{Layout, Row};
use std::ptr;

impl Layout {

    pub fn set_bool(&self, row: &mut [u8], index: usize, value: bool) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Bool);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut bool, value) }
    }

    pub fn set_f32(&self, row: &mut [u8], index: usize, value: f32) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Float4);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut f32, value) }
    }

    pub fn set_f64(&self, row: &mut [u8], index: usize, value: f64) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Float8);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut f64, value) }
    }

    pub fn set_i8(&self, row: &mut [u8], index: usize, value: i8) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int1);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut i8, value) }
    }

    pub fn set_i16(&self, row: &mut [u8], index: usize, value: i16) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int2);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut i16, value) }
    }

    pub fn set_i32(&self, row: &mut [u8], index: usize, value: i32) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int4);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut i32, value) }
    }

    pub fn set_i64(&self, row: &mut [u8], index: usize, value: i64) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int8);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut i64, value) }
    }

    pub fn set_i128(&self, row: &mut [u8], index: usize, value: i128) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Int16);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut i128, value) }
    }

    pub fn set_str(&self, row: &mut [u8], index: usize, value: &str) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::String);

        let bytes = value.as_bytes();
        let len = bytes.len().min(254);

        let dst = unsafe { row.as_mut_ptr().add(field.offset) };
        unsafe {
            *dst = len as u8; // length byte
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst.add(1), len);
        }
    }

    pub fn set_u8(&self, row: &mut [u8], index: usize, value: u8) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint1);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut u8, value) }
    }

    pub fn set_u16(&self, row: &mut [u8], index: usize, value: u16) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint2);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut u16, value) }
    }

    pub fn set_u32(&self, row: &mut [u8], index: usize, value: u32) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint4);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut u32, value) }
    }

    pub fn set_u64(&self, row: &mut [u8], index: usize, value: u64) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint8);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut u64, value) }
    }

    pub fn set_u128(&self, row: &mut [u8], index: usize, value: u128) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);
        debug_assert_eq!(field.value, ValueKind::Uint16);
        unsafe { ptr::write_unaligned(row.as_mut_ptr().add(field.offset) as *mut u128, value) }
    }
}
