// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_codec::encoded::shape::RowShape;
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_value::value::{
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
	let mock = MockClock::from_millis(1000);
	let clock = Clock::Mock(mock.clone());
	let rng = Rng::seeded(42);
	(mock, clock, rng)
}

#[test]
fn test_mixed_type_stress() {
	let (_, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(&[
		ValueType::Boolean,
		ValueType::Int1,
		ValueType::Int2,
		ValueType::Int4,
		ValueType::Int8,
		ValueType::Int16,
		ValueType::Uint1,
		ValueType::Uint2,
		ValueType::Uint4,
		ValueType::Uint8,
		ValueType::Uint16,
		ValueType::Float4,
		ValueType::Float8,
		ValueType::Utf8,
		ValueType::Blob,
		ValueType::Date,
		ValueType::DateTime,
		ValueType::Time,
		ValueType::Duration,
		ValueType::Uuid4,
		ValueType::Uuid7,
		ValueType::IdentityId,
		ValueType::Int,
		ValueType::Uint,
		ValueType::Decimal,
	]);

	let mut row = shape.allocate();

	shape.set::<bool>(&mut row, 0, true);
	shape.set::<i8>(&mut row, 1, -128i8);
	shape.set::<i16>(&mut row, 2, -32768i16);
	shape.set::<i32>(&mut row, 3, -2147483648i32);
	shape.set::<i64>(&mut row, 4, i64::MIN);
	shape.set::<i128>(&mut row, 5, i128::MIN);
	shape.set::<u8>(&mut row, 6, 255u8);
	shape.set::<u16>(&mut row, 7, 65535u16);
	shape.set::<u32>(&mut row, 8, 4294967295u32);
	shape.set::<u64>(&mut row, 9, 18446744073709551615u64);
	shape.set::<u128>(&mut row, 10, u128::MAX);
	shape.set::<f32>(&mut row, 11, f32::MIN);
	shape.set::<f64>(&mut row, 12, f64::MAX);
	shape.set_utf8(&mut row, 13, "stress test 🎭");
	shape.set_blob(&mut row, 14, &Blob::from(vec![0, 255, 127, 128]));
	shape.set::<Date>(&mut row, 15, Date::from_ymd(2024, 12, 25).unwrap());
	shape.set::<DateTime>(&mut row, 16, DateTime::from_epoch_secs(0).unwrap());
	shape.set::<Time>(&mut row, 17, Time::from_hms(23, 59, 59).unwrap());
	shape.set::<Duration>(&mut row, 18, Duration::from_days(365).unwrap());
	shape.set::<Uuid4>(&mut row, 19, Uuid4::generate());
	shape.set::<Uuid7>(&mut row, 20, Uuid7::generate(&clock, &rng));
	shape.set::<IdentityId>(&mut row, 21, IdentityId::generate(&clock, &rng));
	shape.set_int(&mut row, 22, &Int::from(i128::MAX));
	shape.set_uint(&mut row, 23, &Uint::from(u128::MAX));
	shape.set_decimal(&mut row, 24, &Decimal::from_str("123.45").unwrap());

	assert_eq!(shape.get::<bool>(&row, 0), true);
	assert_eq!(shape.get::<i8>(&row, 1), -128);
	assert_eq!(shape.get::<i16>(&row, 2), -32768);
	assert_eq!(shape.get::<i32>(&row, 3), -2147483648);
	assert_eq!(shape.get::<i64>(&row, 4), -9223372036854775808);
	assert_eq!(shape.get::<i128>(&row, 5), i128::MIN);
	assert_eq!(shape.get::<u8>(&row, 6), 255);
	assert_eq!(shape.get::<u16>(&row, 7), 65535);
	assert_eq!(shape.get::<u32>(&row, 8), 4294967295);
	assert_eq!(shape.get::<u64>(&row, 9), 18446744073709551615);
	assert_eq!(shape.get::<u128>(&row, 10), u128::MAX);
	assert_eq!(shape.get::<f32>(&row, 11), f32::MIN);
	assert_eq!(shape.get::<f64>(&row, 12), f64::MAX);
	assert_eq!(shape.get_utf8(&row, 13), "stress test 🎭");
	assert_eq!(shape.get_blob(&row, 14), Blob::from(vec![0, 255, 127, 128]));
	assert_eq!(shape.get::<Date>(&row, 15), Date::from_ymd(2024, 12, 25).unwrap());
	assert_eq!(shape.get::<DateTime>(&row, 16), DateTime::from_epoch_secs(0).unwrap());
	assert_eq!(shape.get::<Time>(&row, 17), Time::from_hms(23, 59, 59).unwrap());
	assert_eq!(shape.get::<Duration>(&row, 18), Duration::from_days(365).unwrap());
	// The uuid values are generated, so only their presence can be asserted.
	assert!(row.is_defined(19));
	assert!(row.is_defined(20));
	assert!(row.is_defined(21));
	assert_eq!(shape.get_int(&row, 22), Int::from(i128::MAX));
	assert_eq!(shape.get_uint(&row, 23), Uint::from(u128::MAX));
	assert_eq!(shape.get_decimal(&row, 24), Decimal::from_str("123.45").unwrap());
}
