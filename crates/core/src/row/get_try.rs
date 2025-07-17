// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::row::{EncodedRow, Layout};
use crate::{Date, DateTime, Interval, Time};

impl Layout {
    pub fn try_get_bool(&self, row: &EncodedRow, index: usize) -> Option<bool> {
        if row.is_defined(index) { Some(self.get_bool(row, index)) } else { None }
    }

    pub fn try_get_f32(&self, row: &EncodedRow, index: usize) -> Option<f32> {
        if row.is_defined(index) { Some(self.get_f32(row, index)) } else { None }
    }

    pub fn try_get_f64(&self, row: &EncodedRow, index: usize) -> Option<f64> {
        if row.is_defined(index) { Some(self.get_f64(row, index)) } else { None }
    }

    pub fn try_get_i8(&self, row: &EncodedRow, index: usize) -> Option<i8> {
        if row.is_defined(index) { Some(self.get_i8(row, index)) } else { None }
    }

    pub fn try_get_i16(&self, row: &EncodedRow, index: usize) -> Option<i16> {
        if row.is_defined(index) { Some(self.get_i16(row, index)) } else { None }
    }

    pub fn try_get_i32(&self, row: &EncodedRow, index: usize) -> Option<i32> {
        if row.is_defined(index) { Some(self.get_i32(row, index)) } else { None }
    }

    pub fn try_get_i64(&self, row: &EncodedRow, index: usize) -> Option<i64> {
        if row.is_defined(index) { Some(self.get_i64(row, index)) } else { None }
    }

    pub fn try_get_i128(&self, row: &EncodedRow, index: usize) -> Option<i128> {
        if row.is_defined(index) { Some(self.get_i128(row, index)) } else { None }
    }

    pub fn try_get_utf8<'a>(&'a self, row: &'a EncodedRow, index: usize) -> Option<&'a str> {
        if row.is_defined(index) { Some(self.get_utf8(row, index)) } else { None }
    }

    pub fn try_get_u8(&self, row: &EncodedRow, index: usize) -> Option<u8> {
        if row.is_defined(index) { Some(self.get_u8(row, index)) } else { None }
    }

    pub fn try_get_u16(&self, row: &EncodedRow, index: usize) -> Option<u16> {
        if row.is_defined(index) { Some(self.get_u16(row, index)) } else { None }
    }

    pub fn try_get_u32(&self, row: &EncodedRow, index: usize) -> Option<u32> {
        if row.is_defined(index) { Some(self.get_u32(row, index)) } else { None }
    }

    pub fn try_get_u64(&self, row: &EncodedRow, index: usize) -> Option<u64> {
        if row.is_defined(index) { Some(self.get_u64(row, index)) } else { None }
    }

    pub fn try_get_u128(&self, row: &EncodedRow, index: usize) -> Option<u128> {
        if row.is_defined(index) { Some(self.get_u128(row, index)) } else { None }
    }

    pub fn try_get_date(&self, row: &EncodedRow, index: usize) -> Option<Date> {
        if row.is_defined(index) { Some(self.get_date(row, index)) } else { None }
    }

    pub fn try_get_datetime(&self, row: &EncodedRow, index: usize) -> Option<DateTime> {
        if row.is_defined(index) { Some(self.get_datetime(row, index)) } else { None }
    }

    pub fn try_get_time(&self, row: &EncodedRow, index: usize) -> Option<Time> {
        if row.is_defined(index) { Some(self.get_time(row, index)) } else { None }
    }

    pub fn try_get_interval(&self, row: &EncodedRow, index: usize) -> Option<Interval> {
        if row.is_defined(index) { Some(self.get_interval(row, index)) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use crate::row::Layout;
    use crate::{Type, Date, DateTime, Interval, Time};

    #[test]
    fn test_try_get_bool() {
        let layout = Layout::new(&[Type::Bool]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_bool(&row, 0), None);

        layout.set_bool(&mut row, 0, true);
        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
    }

    #[test]
    fn test_try_get_f32() {
        let layout = Layout::new(&[Type::Float4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_f32(&row, 0), None);

        layout.set_f32(&mut row, 0, 1.25f32);
        assert_eq!(layout.try_get_f32(&row, 0), Some(1.25f32));
    }

    #[test]
    fn test_try_get_f64() {
        let layout = Layout::new(&[Type::Float8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_f64(&row, 0), None);

        layout.set_f64(&mut row, 0, 2.5f64);
        assert_eq!(layout.try_get_f64(&row, 0), Some(2.5f64));
    }

    #[test]
    fn test_try_get_i8() {
        let layout = Layout::new(&[Type::Int1]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i8(&row, 0), None);

        layout.set_i8(&mut row, 0, 42i8);
        assert_eq!(layout.try_get_i8(&row, 0), Some(42i8));
    }

    #[test]
    fn test_try_get_i16() {
        let layout = Layout::new(&[Type::Int2]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i16(&row, 0), None);

        layout.set_i16(&mut row, 0, -1234i16);
        assert_eq!(layout.try_get_i16(&row, 0), Some(-1234i16));
    }

    #[test]
    fn test_try_get_i32() {
        let layout = Layout::new(&[Type::Int4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i32(&row, 0), None);

        layout.set_i32(&mut row, 0, 56789i32);
        assert_eq!(layout.try_get_i32(&row, 0), Some(56789i32));
    }

    #[test]
    fn test_try_get_i64() {
        let layout = Layout::new(&[Type::Int8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i64(&row, 0), None);

        layout.set_i64(&mut row, 0, -987654321i64);
        assert_eq!(layout.try_get_i64(&row, 0), Some(-987654321i64));
    }

    #[test]
    fn test_try_get_i128() {
        let layout = Layout::new(&[Type::Int16]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_i128(&row, 0), None);

        layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
        assert_eq!(layout.try_get_i128(&row, 0), Some(123456789012345678901234567890i128));
    }

    #[test]
    fn test_try_get_str() {
        let layout = Layout::new(&[Type::Utf8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_utf8(&row, 0), None);

        layout.set_utf8(&mut row, 0, "reifydb");
        assert_eq!(layout.try_get_utf8(&row, 0), Some("reifydb"));
    }

    #[test]
    fn test_try_get_u8() {
        let layout = Layout::new(&[Type::Uint1]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u8(&row, 0), None);

        layout.set_u8(&mut row, 0, 255u8);
        assert_eq!(layout.try_get_u8(&row, 0), Some(255u8));
    }

    #[test]
    fn test_try_get_u16() {
        let layout = Layout::new(&[Type::Uint2]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u16(&row, 0), None);

        layout.set_u16(&mut row, 0, 65535u16);
        assert_eq!(layout.try_get_u16(&row, 0), Some(65535u16));
    }

    #[test]
    fn test_try_get_u32() {
        let layout = Layout::new(&[Type::Uint4]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u32(&row, 0), None);

        layout.set_u32(&mut row, 0, 4294967295u32);
        assert_eq!(layout.try_get_u32(&row, 0), Some(4294967295u32));
    }

    #[test]
    fn test_try_get_u64() {
        let layout = Layout::new(&[Type::Uint8]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u64(&row, 0), None);

        layout.set_u64(&mut row, 0, 18446744073709551615u64);
        assert_eq!(layout.try_get_u64(&row, 0), Some(18446744073709551615u64));
    }

    #[test]
    fn test_try_get_u128() {
        let layout = Layout::new(&[Type::Uint16]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_u128(&row, 0), None);

        layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
        assert_eq!(layout.try_get_u128(&row, 0), Some(340282366920938463463374607431768211455u128));
    }

    #[test]
    fn test_try_get_mixed_utf8_and_static_fields() {
        let layout = Layout::new(&[Type::Bool, Type::Utf8, Type::Int4]);
        let mut row = layout.allocate_row();

        // Initially all fields undefined
        assert_eq!(layout.try_get_bool(&row, 0), None);
        assert_eq!(layout.try_get_utf8(&row, 1), None);
        assert_eq!(layout.try_get_i32(&row, 2), None);

        // Set only some fields
        layout.set_bool(&mut row, 0, true);
        layout.set_utf8(&mut row, 1, "hello");

        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
        assert_eq!(layout.try_get_utf8(&row, 1), Some("hello"));
        assert_eq!(layout.try_get_i32(&row, 2), None); // Still undefined

        // Set remaining field
        layout.set_i32(&mut row, 2, 42);
        assert_eq!(layout.try_get_i32(&row, 2), Some(42));
    }

    #[test]
    fn test_try_get_multiple_utf8_different_sizes() {
        let layout = Layout::new(&[
            Type::Utf8,
            Type::Int2,
            Type::Utf8,
            Type::Bool,
            Type::Utf8,
        ]);
        let mut row = layout.allocate_row();

        // All initially undefined
        assert_eq!(layout.try_get_utf8(&row, 0), None);
        assert_eq!(layout.try_get_utf8(&row, 2), None);
        assert_eq!(layout.try_get_utf8(&row, 4), None);

        layout.set_utf8(&mut row, 0, ""); // Empty string
        layout.set_i16(&mut row, 1, -100i16);
        layout.set_utf8(&mut row, 2, "medium length string");
        layout.set_bool(&mut row, 3, false);
        // Skip field 4 (UTF8)

        assert_eq!(layout.try_get_utf8(&row, 0), Some(""));
        assert_eq!(layout.try_get_i16(&row, 1), Some(-100));
        assert_eq!(layout.try_get_utf8(&row, 2), Some("medium length string"));
        assert_eq!(layout.try_get_bool(&row, 3), Some(false));
        assert_eq!(layout.try_get_utf8(&row, 4), None); // Still undefined

        // Set the last field
        layout.set_utf8(&mut row, 4, "x");
        assert_eq!(layout.try_get_utf8(&row, 4), Some("x"));
    }

    #[test]
    fn test_try_get_sparse_field_setting() {
        let layout = Layout::new(&[Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8]);
        let mut row = layout.allocate_row();

        // Only set some UTF8 fields, leave others undefined
        layout.set_utf8(&mut row, 0, "first");
        layout.set_utf8(&mut row, 2, "third");
        // Skip fields 1 and 3

        assert_eq!(layout.try_get_utf8(&row, 0), Some("first"));
        assert_eq!(layout.try_get_utf8(&row, 1), None);
        assert_eq!(layout.try_get_utf8(&row, 2), Some("third"));
        assert_eq!(layout.try_get_utf8(&row, 3), None);
    }

    #[test]
    fn test_try_get_unicode_multibyte_strings() {
        let layout = Layout::new(&[Type::Utf8, Type::Float8, Type::Utf8]);
        let mut row = layout.allocate_row();

        // Initially undefined
        assert_eq!(layout.try_get_utf8(&row, 0), None);
        assert_eq!(layout.try_get_utf8(&row, 2), None);

        layout.set_utf8(&mut row, 0, "🚀✨🌟");
        layout.set_f64(&mut row, 1, 3.14159);
        // Skip field 2

        assert_eq!(layout.try_get_utf8(&row, 0), Some("🚀✨🌟"));
        assert_eq!(layout.try_get_f64(&row, 1), Some(3.14159));
        assert_eq!(layout.try_get_utf8(&row, 2), None);

        // Set the remaining field
        layout.set_utf8(&mut row, 2, "Hello 世界 🎉");
        assert_eq!(layout.try_get_utf8(&row, 2), Some("Hello 世界 🎉"));
    }

    #[test]
    fn test_try_get_after_set_undefined() {
        let layout = Layout::new(&[Type::Bool, Type::Utf8, Type::Int4]);
        let mut row = layout.allocate_row();

        // Set all fields
        layout.set_bool(&mut row, 0, true);
        layout.set_utf8(&mut row, 1, "test_string");
        layout.set_i32(&mut row, 2, 999);

        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
        assert_eq!(layout.try_get_utf8(&row, 1), Some("test_string"));
        assert_eq!(layout.try_get_i32(&row, 2), Some(999));

        // Set some fields as undefined
        layout.set_undefined(&mut row, 0);
        layout.set_undefined(&mut row, 1);

        assert_eq!(layout.try_get_bool(&row, 0), None);
        assert_eq!(layout.try_get_utf8(&row, 1), None);
        assert_eq!(layout.try_get_i32(&row, 2), Some(999)); // Still defined
    }

    #[test]
    fn test_try_get_empty_and_large_utf8_strings() {
        let layout = Layout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
        let mut row = layout.allocate_row();

        let large_string = "A".repeat(1000);

        // Set in arbitrary order
        layout.set_utf8(&mut row, 1, &large_string);
        layout.set_utf8(&mut row, 2, "small");
        // Skip field 0

        assert_eq!(layout.try_get_utf8(&row, 0), None);
        assert_eq!(layout.try_get_utf8(&row, 1), Some(large_string.as_str()));
        assert_eq!(layout.try_get_utf8(&row, 2), Some("small"));

        // Set the empty string
        layout.set_utf8(&mut row, 0, "");
        assert_eq!(layout.try_get_utf8(&row, 0), Some(""));
    }

    #[test]
    fn test_try_get_static_only_fields_no_dynamic() {
        let layout = Layout::new(&[Type::Bool, Type::Int4, Type::Float8]);
        let mut row = layout.allocate_row();

        // Set only some static fields
        layout.set_bool(&mut row, 0, true);
        layout.set_f64(&mut row, 2, 2.71828);
        // Skip field 1

        // Verify no dynamic section
        assert_eq!(layout.dynamic_section_size(&row), 0);
        assert_eq!(row.len(), layout.total_static_size());

        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
        assert_eq!(layout.try_get_i32(&row, 1), None);
        assert_eq!(layout.try_get_f64(&row, 2), Some(2.71828));
    }

    #[test]
    fn test_try_get_interleaved_static_and_dynamic_setting() {
        let layout = Layout::new(&[Type::Bool, Type::Utf8, Type::Int4, Type::Utf8]);
        let mut row = layout.allocate_row();

        // Interleave static and dynamic field setting, with some undefined
        layout.set_bool(&mut row, 0, true);
        layout.set_utf8(&mut row, 1, "first_string");
        layout.set_i32(&mut row, 2, 999);
        // Skip field 3 (UTF8)

        assert_eq!(layout.try_get_bool(&row, 0), Some(true));
        assert_eq!(layout.try_get_utf8(&row, 1), Some("first_string"));
        assert_eq!(layout.try_get_i32(&row, 2), Some(999));
        assert_eq!(layout.try_get_utf8(&row, 3), None);
    }

    #[test]
    fn test_try_get_date() {
        let layout = Layout::new(&[Type::Date]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_date(&row, 0), None);

        let test_date = Date::from_ymd(2025, 1, 15).unwrap();
        layout.set_date(&mut row, 0, test_date.clone());
        assert_eq!(layout.try_get_date(&row, 0), Some(test_date));
    }

    #[test]
    fn test_try_get_datetime() {
        let layout = Layout::new(&[Type::DateTime]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_datetime(&row, 0), None);

        let test_datetime = DateTime::from_timestamp(1642694400).unwrap();
        layout.set_datetime(&mut row, 0, test_datetime.clone());
        assert_eq!(layout.try_get_datetime(&row, 0), Some(test_datetime));
    }

    #[test]
    fn test_try_get_time() {
        let layout = Layout::new(&[Type::Time]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_time(&row, 0), None);

        let test_time = Time::from_hms(14, 30, 45).unwrap();
        layout.set_time(&mut row, 0, test_time.clone());
        assert_eq!(layout.try_get_time(&row, 0), Some(test_time));
    }

    #[test]
    fn test_try_get_interval() {
        let layout = Layout::new(&[Type::Interval]);
        let mut row = layout.allocate_row();

        assert_eq!(layout.try_get_interval(&row, 0), None);

        let test_interval = Interval::from_days(30);
        layout.set_interval(&mut row, 0, test_interval.clone());
        assert_eq!(layout.try_get_interval(&row, 0), Some(test_interval));
    }

    #[test]
    fn test_try_get_mixed_temporal_fields() {
        let layout =
            Layout::new(&[Type::Date, Type::DateTime, Type::Time, Type::Interval]);
        let mut row = layout.allocate_row();

        // Initially all fields undefined
        assert_eq!(layout.try_get_date(&row, 0), None);
        assert_eq!(layout.try_get_datetime(&row, 1), None);
        assert_eq!(layout.try_get_time(&row, 2), None);
        assert_eq!(layout.try_get_interval(&row, 3), None);

        // Set only some fields
        let test_date = Date::from_ymd(2025, 7, 15).unwrap();
        let test_time = Time::from_hms(9, 15, 30).unwrap();

        layout.set_date(&mut row, 0, test_date.clone());
        layout.set_time(&mut row, 2, test_time.clone());

        assert_eq!(layout.try_get_date(&row, 0), Some(test_date.clone()));
        assert_eq!(layout.try_get_datetime(&row, 1), None);
        assert_eq!(layout.try_get_time(&row, 2), Some(test_time.clone()));
        assert_eq!(layout.try_get_interval(&row, 3), None);

        // Set remaining fields
        let test_datetime = DateTime::from_timestamp(1721030130).unwrap();
        let test_interval = Interval::from_hours(24);

        layout.set_datetime(&mut row, 1, test_datetime.clone());
        layout.set_interval(&mut row, 3, test_interval.clone());

        assert_eq!(layout.try_get_datetime(&row, 1), Some(test_datetime));
        assert_eq!(layout.try_get_interval(&row, 3), Some(test_interval));
    }

    #[test]
    fn test_try_get_temporal_after_set_undefined() {
        let layout =
            Layout::new(&[Type::Date, Type::DateTime, Type::Time, Type::Interval]);
        let mut row = layout.allocate_row();

        // Set all temporal fields
        let test_date = Date::from_ymd(2025, 12, 25).unwrap();
        let test_datetime = DateTime::from_timestamp(1735142400).unwrap();
        let test_time = Time::from_hms(12, 0, 0).unwrap();
        let test_interval = Interval::from_weeks(2);

        layout.set_date(&mut row, 0, test_date.clone());
        layout.set_datetime(&mut row, 1, test_datetime.clone());
        layout.set_time(&mut row, 2, test_time.clone());
        layout.set_interval(&mut row, 3, test_interval.clone());

        assert_eq!(layout.try_get_date(&row, 0), Some(test_date));
        assert_eq!(layout.try_get_datetime(&row, 1), Some(test_datetime.clone()));
        assert_eq!(layout.try_get_time(&row, 2), Some(test_time));
        assert_eq!(layout.try_get_interval(&row, 3), Some(test_interval.clone()));

        // Set some fields as undefined
        layout.set_undefined(&mut row, 0);
        layout.set_undefined(&mut row, 2);

        assert_eq!(layout.try_get_date(&row, 0), None);
        assert_eq!(layout.try_get_datetime(&row, 1), Some(test_datetime));
        assert_eq!(layout.try_get_time(&row, 2), None);
        assert_eq!(layout.try_get_interval(&row, 3), Some(test_interval));
    }
}
