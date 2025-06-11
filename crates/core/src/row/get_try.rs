// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::row::{Layout, Row};

impl Layout {
    pub fn try_get_bool(&self, row: &Row, index: usize) -> Option<bool> {
        if row.is_defined(index) { Some(self.get_bool(row, index)) } else { None }
    }

    pub fn try_get_f32(&self, row: &Row, index: usize) -> Option<f32> {
        if row.is_defined(index) { Some(self.get_f32(row, index)) } else { None }
    }

    pub fn try_get_f64(&self, row: &Row, index: usize) -> Option<f64> {
        if row.is_defined(index) { Some(self.get_f64(row, index)) } else { None }
    }

    pub fn try_get_i8(&self, row: &Row, index: usize) -> Option<i8> {
        if row.is_defined(index) { Some(self.get_i8(row, index)) } else { None }
    }

    pub fn try_get_i16(&self, row: &Row, index: usize) -> Option<i16> {
        if row.is_defined(index) { Some(self.get_i16(row, index)) } else { None }
    }

    pub fn try_get_i32(&self, row: &Row, index: usize) -> Option<i32> {
        if row.is_defined(index) { Some(self.get_i32(row, index)) } else { None }
    }

    pub fn try_get_i64(&self, row: &Row, index: usize) -> Option<i64> {
        if row.is_defined(index) { Some(self.get_i64(row, index)) } else { None }
    }

    pub fn try_get_i128(&self, row: &Row, index: usize) -> Option<i128> {
        if row.is_defined(index) { Some(self.get_i128(row, index)) } else { None }
    }

    pub fn try_get_str(&self, row: &Row, index: usize) -> Option<&str> {
        if row.is_defined(index) { Some(self.get_str(row, index)) } else { None }
    }

    pub fn try_get_u8(&self, row: &Row, index: usize) -> Option<u8> {
        if row.is_defined(index) { Some(self.get_u8(row, index)) } else { None }
    }

    pub fn try_get_u16(&self, row: &Row, index: usize) -> Option<u16> {
        if row.is_defined(index) { Some(self.get_u16(row, index)) } else { None }
    }

    pub fn try_get_u32(&self, row: &Row, index: usize) -> Option<u32> {
        if row.is_defined(index) { Some(self.get_u32(row, index)) } else { None }
    }

    pub fn try_get_u64(&self, row: &Row, index: usize) -> Option<u64> {
        if row.is_defined(index) { Some(self.get_u64(row, index)) } else { None }
    }

    pub fn try_get_u128(&self, row: &Row, index: usize) -> Option<u128> {
        if row.is_defined(index) { Some(self.get_u128(row, index)) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use crate::ValueKind;
    use crate::row::Layout;

    #[test]
    fn test_try_get_bool() {
        let layout = Layout::new(&[ValueKind::Bool]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_bool(&row, 0), None);

        layout.set_bool(&mut row, 0, true);
        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
    }

    #[test]
    fn test_try_get_f32() {
        let layout = Layout::new(&[ValueKind::Float4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_f32(&row, 0), None);

        layout.set_f32(&mut row, 0, 1.25f32);
        assert_eq!(layout.try_get_f32(&row, 0), Some(1.25f32));
    }

    #[test]
    fn test_try_get_f64() {
        let layout = Layout::new(&[ValueKind::Float8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_f64(&row, 0), None);

        layout.set_f64(&mut row, 0, 2.5f64);
        assert_eq!(layout.try_get_f64(&row, 0), Some(2.5f64));
    }

    #[test]
    fn test_try_get_i8() {
        let layout = Layout::new(&[ValueKind::Int1]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i8(&row, 0), None);

        layout.set_i8(&mut row, 0, 42i8);
        assert_eq!(layout.try_get_i8(&row, 0), Some(42i8));
    }

    #[test]
    fn test_try_get_i16() {
        let layout = Layout::new(&[ValueKind::Int2]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i16(&row, 0), None);

        layout.set_i16(&mut row, 0, -1234i16);
        assert_eq!(layout.try_get_i16(&row, 0), Some(-1234i16));
    }

    #[test]
    fn test_try_get_i32() {
        let layout = Layout::new(&[ValueKind::Int4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i32(&row, 0), None);

        layout.set_i32(&mut row, 0, 56789i32);
        assert_eq!(layout.try_get_i32(&row, 0), Some(56789i32));
    }

    #[test]
    fn test_try_get_i64() {
        let layout = Layout::new(&[ValueKind::Int8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i64(&row, 0), None);

        layout.set_i64(&mut row, 0, -987654321i64);
        assert_eq!(layout.try_get_i64(&row, 0), Some(-987654321i64));
    }

    #[test]
    fn test_try_get_i128() {
        let layout = Layout::new(&[ValueKind::Int16]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i128(&row, 0), None);

        layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
        assert_eq!(layout.try_get_i128(&row, 0), Some(123456789012345678901234567890i128));
    }

    #[test]
    fn test_try_get_str() {
        let layout = Layout::new(&[ValueKind::String]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_str(&row, 0), None);

        layout.set_str(&mut row, 0, "reifydb");
        assert_eq!(layout.try_get_str(&row, 0), Some("reifydb"));
    }

    #[test]
    fn test_try_get_u8() {
        let layout = Layout::new(&[ValueKind::Uint1]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u8(&row, 0), None);

        layout.set_u8(&mut row, 0, 255u8);
        assert_eq!(layout.try_get_u8(&row, 0), Some(255u8));
    }

    #[test]
    fn test_try_get_u16() {
        let layout = Layout::new(&[ValueKind::Uint2]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u16(&row, 0), None);

        layout.set_u16(&mut row, 0, 65535u16);
        assert_eq!(layout.try_get_u16(&row, 0), Some(65535u16));
    }

    #[test]
    fn test_try_get_u32() {
        let layout = Layout::new(&[ValueKind::Uint4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u32(&row, 0), None);

        layout.set_u32(&mut row, 0, 4294967295u32);
        assert_eq!(layout.try_get_u32(&row, 0), Some(4294967295u32));
    }

    #[test]
    fn test_try_get_u64() {
        let layout = Layout::new(&[ValueKind::Uint8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u64(&row, 0), None);

        layout.set_u64(&mut row, 0, 18446744073709551615u64);
        assert_eq!(layout.try_get_u64(&row, 0), Some(18446744073709551615u64));
    }

    #[test]
    fn test_try_get_u128() {
        let layout = Layout::new(&[ValueKind::Uint16]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u128(&row, 0), None);

        layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
        assert_eq!(layout.try_get_u128(&row, 0), Some(340282366920938463463374607431768211455u128));
    }
}
