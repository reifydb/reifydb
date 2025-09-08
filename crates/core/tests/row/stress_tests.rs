// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Stress tests for the row encoding system

use reifydb_core::row::EncodedRowLayout;
use reifydb_type::*;

#[test]
fn test_mixed_type_stress() {
	// Comprehensive test with all types interacting
	let layout = EncodedRowLayout::new(&[
		Type::Boolean,
		Type::Int1,
		Type::Int2,
		Type::Int4,
		Type::Int8,
		Type::Int16,
		Type::Uint1,
		Type::Uint2,
		Type::Uint4,
		Type::Uint8,
		Type::Uint16,
		Type::Float4,
		Type::Float8,
		Type::Utf8,
		Type::Blob,
		Type::Date,
		Type::DateTime,
		Type::Time,
		Type::Interval,
		Type::Uuid4,
		Type::Uuid7,
		Type::IdentityId,
		Type::VarInt,
		Type::VarUint,
		Type::Decimal,
	]);

	let mut row = layout.allocate_row();

	// Set all fields
	layout.set_bool(&mut row, 0, true);
	layout.set_i8(&mut row, 1, -128);
	layout.set_i16(&mut row, 2, -32768i16);
	layout.set_i32(&mut row, 3, -2147483648);
	layout.set_i64(&mut row, 4, i64::MIN);
	layout.set_i128(&mut row, 5, i128::MIN);
	layout.set_u8(&mut row, 6, 255);
	layout.set_u16(&mut row, 7, 65535u16);
	layout.set_u32(&mut row, 8, 4294967295u32);
	layout.set_u64(&mut row, 9, 18446744073709551615u64);
	layout.set_u128(&mut row, 10, u128::MAX);
	layout.set_f32(&mut row, 11, f32::MIN);
	layout.set_f64(&mut row, 12, f64::MAX);
	layout.set_utf8(&mut row, 13, "stress test 🎭");
	layout.set_blob(&mut row, 14, &Blob::from(vec![0, 255, 127, 128]));
	layout.set_date(&mut row, 15, Date::from_ymd(2024, 12, 25).unwrap());
	layout.set_datetime(&mut row, 16, DateTime::from_timestamp(0).unwrap());
	layout.set_time(&mut row, 17, Time::from_hms(23, 59, 59).unwrap());
	layout.set_interval(&mut row, 18, Interval::from_days(365));
	layout.set_uuid4(&mut row, 19, Uuid4::generate());
	layout.set_uuid7(&mut row, 20, Uuid7::generate());
	layout.set_identity_id(&mut row, 21, IdentityId::generate());
	layout.set_varint(&mut row, 22, &VarInt::from(i128::MAX));
	layout.set_varuint(&mut row, 23, &VarUint::from(u128::MAX));
	layout.set_decimal(
		&mut row,
		24,
		&Decimal::from_str_with_precision("123.45", 10, 2).unwrap(),
	);

	// Verify all fields
	assert_eq!(layout.get_bool(&row, 0), true);
	assert_eq!(layout.get_i8(&row, 1), -128);
	assert_eq!(layout.get_i16(&row, 2), -32768);
	assert_eq!(layout.get_i32(&row, 3), -2147483648);
	assert_eq!(layout.get_i64(&row, 4), -9223372036854775808);
	assert_eq!(layout.get_i128(&row, 5), i128::MIN);
	assert_eq!(layout.get_u8(&row, 6), 255);
	assert_eq!(layout.get_u16(&row, 7), 65535);
	assert_eq!(layout.get_u32(&row, 8), 4294967295);
	assert_eq!(layout.get_u64(&row, 9), 18446744073709551615);
	assert_eq!(layout.get_u128(&row, 10), u128::MAX);
	assert_eq!(layout.get_f32(&row, 11), f32::MIN);
	assert_eq!(layout.get_f64(&row, 12), f64::MAX);
	assert_eq!(layout.get_utf8(&row, 13), "stress test 🎭");
	assert_eq!(
		layout.get_blob(&row, 14),
		Blob::from(vec![0, 255, 127, 128])
	);
	assert_eq!(
		layout.get_date(&row, 15),
		Date::from_ymd(2024, 12, 25).unwrap()
	);
	assert_eq!(
		layout.get_datetime(&row, 16),
		DateTime::from_timestamp(0).unwrap()
	);
	assert_eq!(
		layout.get_time(&row, 17),
		Time::from_hms(23, 59, 59).unwrap()
	);
	assert_eq!(layout.get_interval(&row, 18), Interval::from_days(365));
	// UUIDs are generated, so just check they exist
	assert!(row.is_defined(19));
	assert!(row.is_defined(20));
	assert!(row.is_defined(21));
	assert_eq!(layout.get_varint(&row, 22), VarInt::from(i128::MAX));
	assert_eq!(layout.get_varuint(&row, 23), VarUint::from(u128::MAX));
	assert_eq!(
		layout.get_decimal(&row, 24),
		Decimal::from_str_with_precision("123.45", 10, 2).unwrap()
	);
}
