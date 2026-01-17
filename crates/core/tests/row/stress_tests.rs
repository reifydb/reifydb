// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Stress tests for the encoded encoding system

use std::str::FromStr;

use reifydb_core::encoded::schema::Schema;
use reifydb_type::value::{
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

#[test]
fn test_mixed_type_stress() {
	// Comprehensive test with all types interacting
	let schema = Schema::testing(&[
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
		Type::Duration,
		Type::Uuid4,
		Type::Uuid7,
		Type::IdentityId,
		Type::Int,
		Type::Uint,
		Type::Decimal,
	]);

	let mut row = schema.allocate();

	// Set all fields
	schema.set_bool(&mut row, 0, true);
	schema.set_i8(&mut row, 1, -128);
	schema.set_i16(&mut row, 2, -32768i16);
	schema.set_i32(&mut row, 3, -2147483648);
	schema.set_i64(&mut row, 4, i64::MIN);
	schema.set_i128(&mut row, 5, i128::MIN);
	schema.set_u8(&mut row, 6, 255);
	schema.set_u16(&mut row, 7, 65535u16);
	schema.set_u32(&mut row, 8, 4294967295u32);
	schema.set_u64(&mut row, 9, 18446744073709551615u64);
	schema.set_u128(&mut row, 10, u128::MAX);
	schema.set_f32(&mut row, 11, f32::MIN);
	schema.set_f64(&mut row, 12, f64::MAX);
	schema.set_utf8(&mut row, 13, "stress test 🎭");
	schema.set_blob(&mut row, 14, &Blob::from(vec![0, 255, 127, 128]));
	schema.set_date(&mut row, 15, Date::from_ymd(2024, 12, 25).unwrap());
	schema.set_datetime(&mut row, 16, DateTime::from_timestamp(0).unwrap());
	schema.set_time(&mut row, 17, Time::from_hms(23, 59, 59).unwrap());
	schema.set_duration(&mut row, 18, Duration::from_days(365));
	schema.set_uuid4(&mut row, 19, Uuid4::generate());
	schema.set_uuid7(&mut row, 20, Uuid7::generate());
	schema.set_identity_id(&mut row, 21, IdentityId::generate());
	schema.set_int(&mut row, 22, &Int::from(i128::MAX));
	schema.set_uint(&mut row, 23, &Uint::from(u128::MAX));
	schema.set_decimal(&mut row, 24, &Decimal::from_str("123.45").unwrap());

	// Verify all fields
	assert_eq!(schema.get_bool(&row, 0), true);
	assert_eq!(schema.get_i8(&row, 1), -128);
	assert_eq!(schema.get_i16(&row, 2), -32768);
	assert_eq!(schema.get_i32(&row, 3), -2147483648);
	assert_eq!(schema.get_i64(&row, 4), -9223372036854775808);
	assert_eq!(schema.get_i128(&row, 5), i128::MIN);
	assert_eq!(schema.get_u8(&row, 6), 255);
	assert_eq!(schema.get_u16(&row, 7), 65535);
	assert_eq!(schema.get_u32(&row, 8), 4294967295);
	assert_eq!(schema.get_u64(&row, 9), 18446744073709551615);
	assert_eq!(schema.get_u128(&row, 10), u128::MAX);
	assert_eq!(schema.get_f32(&row, 11), f32::MIN);
	assert_eq!(schema.get_f64(&row, 12), f64::MAX);
	assert_eq!(schema.get_utf8(&row, 13), "stress test 🎭");
	assert_eq!(schema.get_blob(&row, 14), Blob::from(vec![0, 255, 127, 128]));
	assert_eq!(schema.get_date(&row, 15), Date::from_ymd(2024, 12, 25).unwrap());
	assert_eq!(schema.get_datetime(&row, 16), DateTime::from_timestamp(0).unwrap());
	assert_eq!(schema.get_time(&row, 17), Time::from_hms(23, 59, 59).unwrap());
	assert_eq!(schema.get_duration(&row, 18), Duration::from_days(365));
	// UUIDs are generated, so just check they exist
	assert!(row.is_defined(19));
	assert!(row.is_defined(20));
	assert!(row.is_defined(21));
	assert_eq!(schema.get_int(&row, 22), Int::from(i128::MAX));
	assert_eq!(schema.get_uint(&row, 23), Uint::from(u128::MAX));
	assert_eq!(schema.get_decimal(&row, 24), Decimal::from_str("123.45").unwrap());
}
