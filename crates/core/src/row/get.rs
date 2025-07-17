// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;
use crate::row::{EncodedRow, Layout};
use crate::value::{Date, DateTime, Interval, Time};

impl Layout {
    pub fn get_bool(&self, row: &EncodedRow, index: usize) -> bool {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Bool);
        unsafe { (row.as_ptr().add(field.offset) as *const bool).read_unaligned() }
    }

    pub fn get_f32(&self, row: &EncodedRow, index: usize) -> f32 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Float4);
        unsafe { (row.as_ptr().add(field.offset) as *const f32).read_unaligned() }
    }

    pub fn get_f64(&self, row: &EncodedRow, index: usize) -> f64 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Float8);
        unsafe { (row.as_ptr().add(field.offset) as *const f64).read_unaligned() }
    }

    pub fn get_i8(&self, row: &EncodedRow, index: usize) -> i8 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Int1);
        unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
    }

    pub fn get_i16(&self, row: &EncodedRow, index: usize) -> i16 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Int2);
        unsafe { (row.as_ptr().add(field.offset) as *const i16).read_unaligned() }
    }

    pub fn get_i32(&self, row: &EncodedRow, index: usize) -> i32 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Int4);
        unsafe { (row.as_ptr().add(field.offset) as *const i32).read_unaligned() }
    }

    pub fn get_i64(&self, row: &EncodedRow, index: usize) -> i64 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Int8);
        unsafe { (row.as_ptr().add(field.offset) as *const i64).read_unaligned() }
    }

    pub fn get_i128(&self, row: &EncodedRow, index: usize) -> i128 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Int16);
        unsafe { (row.as_ptr().add(field.offset) as *const i128).read_unaligned() }
    }

    pub fn get_utf8<'a>(&'a self, row: &'a EncodedRow, index: usize) -> &'a str {
        let field = &self.fields[index];
        debug_assert_eq!(field.value, Type::Utf8);

        // Read offset and length from static section
        let ref_slice = &row.as_slice()[field.offset..field.offset + 8];
        let offset =
            u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
        let length =
            u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

        // Get string from dynamic section
        let dynamic_start = self.dynamic_section_start();
        let string_start = dynamic_start + offset;
        let string_slice = &row.as_slice()[string_start..string_start + length];

        unsafe { std::str::from_utf8_unchecked(string_slice) }
    }

    pub fn get_u8(&self, row: &EncodedRow, index: usize) -> u8 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Uint1);
        unsafe { row.as_ptr().add(field.offset).read_unaligned() }
    }

    pub fn get_u16(&self, row: &EncodedRow, index: usize) -> u16 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Uint2);
        unsafe { (row.as_ptr().add(field.offset) as *const u16).read_unaligned() }
    }

    pub fn get_u32(&self, row: &EncodedRow, index: usize) -> u32 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Uint4);
        unsafe { (row.as_ptr().add(field.offset) as *const u32).read_unaligned() }
    }

    pub fn get_u64(&self, row: &EncodedRow, index: usize) -> u64 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Uint8);
        unsafe { (row.as_ptr().add(field.offset) as *const u64).read_unaligned() }
    }

    pub fn get_u128(&self, row: &EncodedRow, index: usize) -> u128 {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Uint16);
        unsafe { (row.as_ptr().add(field.offset) as *const u128).read_unaligned() }
    }

    pub fn get_date(&self, row: &EncodedRow, index: usize) -> Date {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Date);
        unsafe {
            Date::from_days_since_epoch(
                (row.as_ptr().add(field.offset) as *const i32).read_unaligned(),
            )
            .unwrap()
        }
    }

    pub fn get_datetime(&self, row: &EncodedRow, index: usize) -> DateTime {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::DateTime);
        unsafe {
            // Read i64 seconds at offset
            let seconds = (row.as_ptr().add(field.offset) as *const i64).read_unaligned();
            // Read u32 nanos at offset + 8
            let nanos = (row.as_ptr().add(field.offset + 8) as *const u32).read_unaligned();
            DateTime::from_parts(seconds, nanos).unwrap()
        }
    }

    pub fn get_time(&self, row: &EncodedRow, index: usize) -> Time {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Time);
        unsafe {
            Time::from_nanos_since_midnight(
                (row.as_ptr().add(field.offset) as *const u64).read_unaligned(),
            )
            .unwrap()
        }
    }

    pub fn get_interval(&self, row: &EncodedRow, index: usize) -> Interval {
        let field = &self.fields[index];
        debug_assert!(row.len() >= self.total_static_size());
        debug_assert_eq!(field.value, Type::Interval);
        unsafe {
            Interval::from_nanos((row.as_ptr().add(field.offset) as *const i64).read_unaligned())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Type;
    use crate::row::Layout;
    use crate::value::{Date, DateTime, Interval, Time};

    #[test]
    fn test_get_bool() {
        let layout = Layout::new(&[Type::Bool]);
        let mut row = layout.allocate_row();
        layout.set_bool(&mut row, 0, true);
        assert!(layout.get_bool(&row, 0));
    }

    #[test]
    fn test_get_f32() {
        let layout = Layout::new(&[Type::Float4]);
        let mut row = layout.allocate_row();
        layout.set_f32(&mut row, 0, 1.25f32);
        assert_eq!(layout.get_f32(&row, 0), 1.25f32);
    }

    #[test]
    fn test_get_f64() {
        let layout = Layout::new(&[Type::Float8]);
        let mut row = layout.allocate_row();
        layout.set_f64(&mut row, 0, 2.5f64);
        assert_eq!(layout.get_f64(&row, 0), 2.5f64);
    }

    #[test]
    fn test_get_i8() {
        let layout = Layout::new(&[Type::Int1]);
        let mut row = layout.allocate_row();
        layout.set_i8(&mut row, 0, 42i8);
        assert_eq!(layout.get_i8(&row, 0), 42i8);
    }

    #[test]
    fn test_get_i16() {
        let layout = Layout::new(&[Type::Int2]);
        let mut row = layout.allocate_row();
        layout.set_i16(&mut row, 0, -1234i16);
        assert_eq!(layout.get_i16(&row, 0), -1234i16);
    }

    #[test]
    fn test_get_i32() {
        let layout = Layout::new(&[Type::Int4]);
        let mut row = layout.allocate_row();
        layout.set_i32(&mut row, 0, 56789i32);
        assert_eq!(layout.get_i32(&row, 0), 56789i32);
    }

    #[test]
    fn test_get_i64() {
        let layout = Layout::new(&[Type::Int8]);
        let mut row = layout.allocate_row();
        layout.set_i64(&mut row, 0, -987654321i64);
        assert_eq!(layout.get_i64(&row, 0), -987654321i64);
    }

    #[test]
    fn test_get_i128() {
        let layout = Layout::new(&[Type::Int16]);
        let mut row = layout.allocate_row();
        layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
        assert_eq!(layout.get_i128(&row, 0), 123456789012345678901234567890i128);
    }

    #[test]
    fn test_get_str() {
        let layout = Layout::new(&[Type::Utf8]);
        let mut row = layout.allocate_row();
        layout.set_utf8(&mut row, 0, "reifydb");
        assert_eq!(layout.get_utf8(&row, 0), "reifydb");
    }

    #[test]
    fn test_get_u8() {
        let layout = Layout::new(&[Type::Uint1]);
        let mut row = layout.allocate_row();
        layout.set_u8(&mut row, 0, 255u8);
        assert_eq!(layout.get_u8(&row, 0), 255u8);
    }

    #[test]
    fn test_get_u16() {
        let layout = Layout::new(&[Type::Uint2]);
        let mut row = layout.allocate_row();
        layout.set_u16(&mut row, 0, 65535u16);
        assert_eq!(layout.get_u16(&row, 0), 65535u16);
    }

    #[test]
    fn test_get_u32() {
        let layout = Layout::new(&[Type::Uint4]);
        let mut row = layout.allocate_row();
        layout.set_u32(&mut row, 0, 4294967295u32);
        assert_eq!(layout.get_u32(&row, 0), 4294967295u32);
    }

    #[test]
    fn test_get_u64() {
        let layout = Layout::new(&[Type::Uint8]);
        let mut row = layout.allocate_row();
        layout.set_u64(&mut row, 0, 18446744073709551615u64);
        assert_eq!(layout.get_u64(&row, 0), 18446744073709551615u64);
    }

    #[test]
    fn test_get_u128() {
        let layout = Layout::new(&[Type::Uint16]);
        let mut row = layout.allocate_row();
        layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
        assert_eq!(layout.get_u128(&row, 0), 340282366920938463463374607431768211455u128);
    }

    #[test]
    fn test_mixed_utf8_and_static_fields() {
        let layout = Layout::new(&[Type::Bool, Type::Utf8, Type::Int4]);
        let mut row = layout.allocate_row();

        layout.set_bool(&mut row, 0, true);
        layout.set_utf8(&mut row, 1, "hello");
        layout.set_i32(&mut row, 2, 42);

        assert_eq!(layout.get_bool(&row, 0), true);
        assert_eq!(layout.get_utf8(&row, 1), "hello");
        assert_eq!(layout.get_i32(&row, 2), 42);
    }

    #[test]
    fn test_multiple_utf8_different_sizes() {
        let layout = Layout::new(&[
            Type::Utf8,
            Type::Int2,
            Type::Utf8,
            Type::Bool,
            Type::Utf8,
        ]);
        let mut row = layout.allocate_row();

        layout.set_utf8(&mut row, 0, "");
        layout.set_i16(&mut row, 1, -100i16);
        layout.set_utf8(&mut row, 2, "medium length string");
        layout.set_bool(&mut row, 3, false);
        layout.set_utf8(&mut row, 4, "x");

        assert_eq!(layout.get_utf8(&row, 0), "");
        assert_eq!(layout.get_i16(&row, 1), -100);
        assert_eq!(layout.get_utf8(&row, 2), "medium length string");
        assert_eq!(layout.get_bool(&row, 3), false);
        assert_eq!(layout.get_utf8(&row, 4), "x");
    }

    #[test]
    fn test_empty_and_large_utf8_strings() {
        let layout = Layout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
        let mut row = layout.allocate_row();

        let large_string = "A".repeat(1000);

        layout.set_utf8(&mut row, 0, "");
        layout.set_utf8(&mut row, 1, &large_string);
        layout.set_utf8(&mut row, 2, "small");

        assert_eq!(layout.get_utf8(&row, 0), "");
        assert_eq!(layout.get_utf8(&row, 1), large_string);
        assert_eq!(layout.get_utf8(&row, 2), "small");
    }

    #[test]
    fn test_unicode_multibyte_strings() {
        let layout = Layout::new(&[Type::Utf8, Type::Float8, Type::Utf8]);
        let mut row = layout.allocate_row();

        layout.set_utf8(&mut row, 0, "🚀✨🌟");
        layout.set_f64(&mut row, 1, 3.14159);
        layout.set_utf8(&mut row, 2, "Hello 世界 🎉");

        assert_eq!(layout.get_utf8(&row, 0), "🚀✨🌟");
        assert_eq!(layout.get_f64(&row, 1), 3.14159);
        assert_eq!(layout.get_utf8(&row, 2), "Hello 世界 🎉");
    }

    #[test]
    fn test_utf8_arbitrary_setting_order() {
        let layout = Layout::new(&[Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8]);
        let mut row = layout.allocate_row();

        // Set in reverse order
        layout.set_utf8(&mut row, 3, "fourth");
        layout.set_utf8(&mut row, 1, "second");
        layout.set_utf8(&mut row, 0, "first");
        layout.set_utf8(&mut row, 2, "third");

        assert_eq!(layout.get_utf8(&row, 0), "first");
        assert_eq!(layout.get_utf8(&row, 1), "second");
        assert_eq!(layout.get_utf8(&row, 2), "third");
        assert_eq!(layout.get_utf8(&row, 3), "fourth");
    }

    #[test]
    fn test_static_only_fields_no_dynamic() {
        let layout = Layout::new(&[Type::Bool, Type::Int4, Type::Float8]);
        let mut row = layout.allocate_row();

        layout.set_bool(&mut row, 0, true);
        layout.set_i32(&mut row, 1, -12345);
        layout.set_f64(&mut row, 2, 2.71828);

        // Verify no dynamic section
        assert_eq!(layout.dynamic_section_size(&row), 0);
        assert_eq!(row.len(), layout.total_static_size());

        assert_eq!(layout.get_bool(&row, 0), true);
        assert_eq!(layout.get_i32(&row, 1), -12345);
        assert_eq!(layout.get_f64(&row, 2), 2.71828);
    }

    #[test]
    fn test_interleaved_static_and_dynamic_setting() {
        let layout = Layout::new(&[Type::Bool, Type::Utf8, Type::Int4, Type::Utf8]);
        let mut row = layout.allocate_row();

        // Interleave static and dynamic field setting
        layout.set_bool(&mut row, 0, true);
        layout.set_utf8(&mut row, 1, "first_string");
        layout.set_i32(&mut row, 2, 999);
        layout.set_utf8(&mut row, 3, "second_string");

        assert_eq!(layout.get_bool(&row, 0), true);
        assert_eq!(layout.get_utf8(&row, 1), "first_string");
        assert_eq!(layout.get_i32(&row, 2), 999);
        assert_eq!(layout.get_utf8(&row, 3), "second_string");
    }

    #[test]
    fn test_date() {
        let layout = Layout::new(&[Type::Date]);
        let mut row = layout.allocate_row();

        let value = Date::new(2021, 1, 1).unwrap();
        layout.set_date(&mut row, 0, value.clone());
        assert_eq!(layout.get_date(&row, 0), value);
    }

    #[test]
    fn test_date_epoch() {
        let layout = Layout::new(&[Type::Date]);
        let mut row = layout.allocate_row();

        let value = Date::default();
        layout.set_date(&mut row, 0, value.clone());
        assert_eq!(layout.get_date(&row, 0), value);
    }

    #[test]
    fn test_datetime() {
        let layout = Layout::new(&[Type::DateTime]);
        let mut row = layout.allocate_row();

        let value = DateTime::new(2024, 9, 9, 08, 17, 0, 1234).unwrap();
        layout.set_datetime(&mut row, 0, value.clone());
        assert_eq!(layout.get_datetime(&row, 0), value);
    }

    #[test]
    fn test_datetime_epoch() {
        let layout = Layout::new(&[Type::DateTime]);
        let mut row = layout.allocate_row();

        let value = DateTime::default();
        layout.set_datetime(&mut row, 0, value.clone());
        assert_eq!(layout.get_datetime(&row, 0), value);
    }

    #[test]
    fn test_time() {
        let layout = Layout::new(&[Type::Time]);
        let mut row = layout.allocate_row();

        let value = Time::new(20, 50, 0, 0).unwrap();
        layout.set_time(&mut row, 0, value.clone());
        assert_eq!(layout.get_time(&row, 0), value);
    }

    #[test]
    fn test_time_midnight() {
        let layout = Layout::new(&[Type::Time]);
        let mut row = layout.allocate_row();

        let value = Time::default();
        layout.set_time(&mut row, 0, value.clone());
        assert_eq!(layout.get_time(&row, 0), value);
    }

    #[test]
    fn test_interval() {
        let layout = Layout::new(&[Type::Interval]);
        let mut row = layout.allocate_row();

        let value = Interval::from_seconds(-7200);
        layout.set_interval(&mut row, 0, value.clone());
        assert_eq!(layout.get_interval(&row, 0), value);
    }

    #[test]
    fn test_interval_zero() {
        let layout = Layout::new(&[Type::Interval]);
        let mut row = layout.allocate_row();

        let value = Interval::default();
        layout.set_interval(&mut row, 0, value.clone());
        assert_eq!(layout.get_interval(&row, 0), value);
    }
}
