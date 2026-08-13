// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{f64, str::FromStr};

use num_bigint::BigInt;
use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer, sort::SortOrder};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_value::{
	util::hex::encode,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
	let mock = MockClock::from_millis(1000);
	let clock = Clock::Mock(mock.clone());
	let rng = Rng::seeded(42);
	(mock, clock, rng)
}

#[test]
fn test_new() {
	let serializer = KeySerializer::new();
	assert!(serializer.is_empty());
	assert_eq!(serializer.len(), 0);
}

#[test]
fn test_with_capacity() {
	let serializer = KeySerializer::with_capacity(100);
	assert!(serializer.is_empty());
	assert_eq!(serializer.len(), 0);
}

#[test]
fn test_extend_bool() {
	let mut serializer = KeySerializer::new();
	serializer.extend_bool(true);
	let result = serializer.finish();
	assert_eq!(result, vec![0x00]);
	assert_eq!(encode(&result), "00");

	let mut serializer = KeySerializer::new();
	serializer.extend_bool(false);
	let result = serializer.finish();
	assert_eq!(result, vec![0x01]);
	assert_eq!(encode(&result), "01");
}

#[test]
fn test_extend_f32() {
	let mut serializer = KeySerializer::new();
	serializer.extend_f32(3.14f32);
	let result = serializer.finish();
	assert_eq!(result.len(), 4);
	assert_eq!(encode(&result), "3fb70a3c");

	let mut serializer = KeySerializer::new();
	serializer.extend_f32(-3.14f32);
	let result = serializer.finish();
	assert_eq!(result.len(), 4);
	assert_eq!(encode(&result), "c048f5c3");

	let mut serializer = KeySerializer::new();
	serializer.extend_f32(0.0f32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_f32(f32::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00800000");

	let mut serializer = KeySerializer::new();
	serializer.extend_f32(f32::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ff7fffff");
}

#[test]
fn test_extend_f64() {
	let mut serializer = KeySerializer::new();
	serializer.extend_f64(f64::consts::PI);
	let result = serializer.finish();
	assert_eq!(result.len(), 8);
	assert_eq!(encode(&result), "3ff6de04abbbd2e7");

	let mut serializer = KeySerializer::new();
	serializer.extend_f64(-f64::consts::PI);
	let result = serializer.finish();
	assert_eq!(result.len(), 8);
	assert_eq!(encode(&result), "c00921fb54442d18");

	let mut serializer = KeySerializer::new();
	serializer.extend_f64(0.0f64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fffffffffffffff");
}

#[test]
fn test_extend_i8() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i8(0i8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7f");

	let mut serializer = KeySerializer::new();
	serializer.extend_i8(1i8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7e");

	let mut serializer = KeySerializer::new();
	serializer.extend_i8(-1i8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "80");

	let mut serializer = KeySerializer::new();
	serializer.extend_i8(i8::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00");

	let mut serializer = KeySerializer::new();
	serializer.extend_i8(i8::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ff");
}

#[test]
fn test_extend_i16() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i16(0i16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fff");

	let mut serializer = KeySerializer::new();
	serializer.extend_i16(1i16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7ffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_i16(-1i16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "8000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i16(i16::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "0000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i16(i16::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffff");
}

#[test]
fn test_extend_i32() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i32(0i32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_i32(1i32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7ffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_i32(-1i32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "80000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i32(i32::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i32(i32::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffff");
}

#[test]
fn test_extend_i64() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i64(0i64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fffffffffffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_i64(1i64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7ffffffffffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_i64(-1i64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "8000000000000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i64(i64::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "0000000000000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i64(i64::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffffffffffff");
}

#[test]
fn test_extend_i128() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i128(0i128);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7fffffffffffffffffffffffffffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_i128(1i128);
	let result = serializer.finish();
	assert_eq!(encode(&result), "7ffffffffffffffffffffffffffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_i128(-1i128);
	let result = serializer.finish();
	assert_eq!(encode(&result), "80000000000000000000000000000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i128(i128::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00000000000000000000000000000000");

	let mut serializer = KeySerializer::new();
	serializer.extend_i128(i128::MIN);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffffffffffffffffffffffffffff");
}

#[test]
fn test_extend_u8() {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(0u8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ff");

	let mut serializer = KeySerializer::new();
	serializer.extend_u8(1u8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "fe");

	let mut serializer = KeySerializer::new();
	serializer.extend_u8(255u8);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00");
}

#[test]
fn test_extend_u16() {
	let mut serializer = KeySerializer::new();
	serializer.extend_u16(0u16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_u16(1u16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "fffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_u16(255u16);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ff00");

	let mut serializer = KeySerializer::new();
	serializer.extend_u16(u16::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "0000");
}

#[test]
fn test_extend_u32() {
	let mut serializer = KeySerializer::new();
	serializer.extend_u32(0u32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_u32(1u32);
	let result = serializer.finish();
	assert_eq!(encode(&result), "fffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_u32(u32::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00000000");
}

#[test]
fn test_extend_u64() {
	let mut serializer = KeySerializer::new();
	serializer.extend_u64(0u64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffffffffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_u64(1u64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "fffffffffffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_u64(65535u64);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffffffff0000");

	let mut serializer = KeySerializer::new();
	serializer.extend_u64(u64::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "0000000000000000");
}

#[test]
fn test_extend_u128() {
	let mut serializer = KeySerializer::new();
	serializer.extend_u128(0u128);
	let result = serializer.finish();
	assert_eq!(encode(&result), "ffffffffffffffffffffffffffffffff");

	let mut serializer = KeySerializer::new();
	serializer.extend_u128(1u128);
	let result = serializer.finish();
	assert_eq!(encode(&result), "fffffffffffffffffffffffffffffffe");

	let mut serializer = KeySerializer::new();
	serializer.extend_u128(u128::MAX);
	let result = serializer.finish();
	assert_eq!(encode(&result), "00000000000000000000000000000000");
}

#[test]
fn test_extend_bytes() {
	// 0xff 0xff terminates a byte string, so a literal 0xff in the data is escaped to
	// 0xff 0x00 and cannot be mistaken for the terminator.
	let mut serializer = KeySerializer::new();
	serializer.extend_bytes(b"hello");
	let result = serializer.finish();
	assert_eq!(result, vec![b'h', b'e', b'l', b'l', b'o', 0xff, 0xff]);

	let mut serializer = KeySerializer::new();
	serializer.extend_bytes(&[0x01, 0xff, 0x02]);
	let result = serializer.finish();
	assert_eq!(result, vec![0x01, 0xff, 0x00, 0x02, 0xff, 0xff]);
}

#[test]
fn test_extend_str() {
	let mut serializer = KeySerializer::new();
	serializer.extend_str("hello world");
	let result = serializer.finish();
	assert!(result.len() > "hello world".len());
	assert!(result.ends_with(&[0xff, 0xff]));
}

#[test]
fn test_extend_raw() {
	let mut serializer = KeySerializer::new();
	serializer.extend_raw(&[0x01, 0x02, 0x03]);
	let result = serializer.finish();
	assert_eq!(result, vec![0x01, 0x02, 0x03]);
}

#[test]
fn test_chaining() {
	let mut serializer = KeySerializer::new();
	serializer.extend_bool(true).extend_i32(42i32).extend_str("test").extend_u64(1000u64);
	let result = serializer.finish();

	assert!(result.len() >= 13);

	let mut de = KeyDeserializer::from_bytes(&result);
	assert_eq!(de.read_bool().unwrap(), true);
	assert_eq!(de.read_i32().unwrap(), 42);
	assert_eq!(de.read_str().unwrap(), "test");
	assert_eq!(de.read_u64().unwrap(), 1000);
	assert!(de.is_empty());
}

#[test]
fn test_ordering_descending_i32() {
	// Keycode is descending: a larger value must encode to smaller bytes so a forward scan
	// returns it first.
	let mut ser1 = KeySerializer::new();
	ser1.extend_i32(1i32);
	let bytes1 = ser1.finish();

	let mut ser2 = KeySerializer::new();
	ser2.extend_i32(100i32);
	let bytes2 = ser2.finish();

	let mut ser3 = KeySerializer::new();
	ser3.extend_i32(1000i32);
	let bytes3 = ser3.finish();

	assert!(bytes3 < bytes2, "encode(1000) should be < encode(100)");
	assert!(bytes2 < bytes1, "encode(100) should be < encode(1)");
}

#[test]
fn test_extend_value_with_direction_ascending() {
	// Ascending: a smaller value must encode to smaller bytes so a forward scan returns it first.
	let enc = |v: i32| {
		let mut s = KeySerializer::new();
		s.extend_value_with_direction(&Value::Int4(v), SortOrder::Asc);
		s.finish()
	};
	assert!(enc(1) < enc(100), "asc: encode(1) should sort before encode(100)");
	assert!(enc(100) < enc(1000), "asc: encode(100) should sort before encode(1000)");
	assert!(enc(-5) < enc(0), "asc: encode(-5) should sort before encode(0)");
}

#[test]
fn test_extend_value_with_direction_descending() {
	// Descending: a larger value must encode to smaller bytes so a forward scan returns it first.
	let enc = |v: i32| {
		let mut s = KeySerializer::new();
		s.extend_value_with_direction(&Value::Int4(v), SortOrder::Desc);
		s.finish()
	};
	assert!(enc(1000) < enc(100), "desc: encode(1000) should sort before encode(100)");
	assert!(enc(100) < enc(1), "desc: encode(100) should sort before encode(1)");
}

#[test]
fn test_extend_value_with_direction_none_policy() {
	// none sorts last under ascending and first under descending.
	let enc = |v: &Value, d: SortOrder| {
		let mut s = KeySerializer::new();
		s.extend_value_with_direction(v, d);
		s.finish()
	};
	let none = Value::none_of(ValueType::Int4);
	let present = Value::Int4(0);
	assert!(enc(&present, SortOrder::Asc) < enc(&none, SortOrder::Asc), "asc: present should sort before none");
	assert!(enc(&none, SortOrder::Desc) < enc(&present, SortOrder::Desc), "desc: none should sort before present");
}

#[test]
fn test_extend_value_with_direction_utf8() {
	// Strings encode ascending in keycode, unlike the numeric types, so asc must preserve
	// lexicographic order and only desc inverts it.
	let enc = |s: &str, d: SortOrder| {
		let mut ser = KeySerializer::new();
		ser.extend_value_with_direction(&Value::Utf8(s.to_string()), d);
		ser.finish()
	};
	assert!(enc("apple", SortOrder::Asc) < enc("banana", SortOrder::Asc), "asc: apple < banana");
	assert!(enc("banana", SortOrder::Asc) < enc("cherry", SortOrder::Asc), "asc: banana < cherry");
	assert!(enc("cherry", SortOrder::Desc) < enc("banana", SortOrder::Desc), "desc: cherry first");
	assert!(enc("banana", SortOrder::Desc) < enc("apple", SortOrder::Desc), "desc: banana before apple");
}

#[test]
fn test_ordering_descending_u64() {
	// Keycode is descending: a larger u64 must encode to smaller bytes.
	let mut ser1 = KeySerializer::new();
	ser1.extend_u64(1u64);
	let bytes1 = ser1.finish();

	let mut ser2 = KeySerializer::new();
	ser2.extend_u64(100u64);
	let bytes2 = ser2.finish();

	let mut ser3 = KeySerializer::new();
	ser3.extend_u64(10000u64);
	let bytes3 = ser3.finish();

	assert!(bytes3 < bytes2, "encode(10000) should be < encode(100)");
	assert!(bytes2 < bytes1, "encode(100) should be < encode(1)");
}

#[test]
fn test_ordering_descending_negative() {
	// The sign flip must keep negatives in the same descending order as positives.
	let mut ser1 = KeySerializer::new();
	ser1.extend_i32(-1i32);
	let bytes_neg1 = ser1.finish();

	let mut ser2 = KeySerializer::new();
	ser2.extend_i32(-100i32);
	let bytes_neg100 = ser2.finish();

	let mut ser3 = KeySerializer::new();
	ser3.extend_i32(-1000i32);
	let bytes_neg1000 = ser3.finish();

	assert!(bytes_neg1 < bytes_neg100, "encode(-1) should be < encode(-100)");
	assert!(bytes_neg100 < bytes_neg1000, "encode(-100) should be < encode(-1000)");
}

#[test]
fn test_ordering_mixed_sign() {
	// The sign flip must order across zero, not just within one sign.
	let mut ser_neg = KeySerializer::new();
	ser_neg.extend_i32(-1i32);
	let bytes_neg = ser_neg.finish();

	let mut ser_zero = KeySerializer::new();
	ser_zero.extend_i32(0i32);
	let bytes_zero = ser_zero.finish();

	let mut ser_pos = KeySerializer::new();
	ser_pos.extend_i32(1i32);
	let bytes_pos = ser_pos.finish();

	assert!(bytes_pos < bytes_zero, "encode(1) should be < encode(0)");
	assert!(bytes_zero < bytes_neg, "encode(0) should be < encode(-1)");
}

#[test]
fn test_date() {
	let mut serializer = KeySerializer::new();
	let date = Date::from_ymd(2024, 1, 1).unwrap();
	serializer.extend_date(&date);
	let result = serializer.finish();
	assert_eq!(result.len(), 4); // i32 encoding
}

#[test]
fn test_datetime() {
	let mut serializer = KeySerializer::new();
	let datetime = DateTime::from_ymd_hms(2024, 1, 1, 12, 0, 0).unwrap();
	serializer.extend_datetime(&datetime);
	let result = serializer.finish();
	assert_eq!(result.len(), 8);
}

#[test]
fn test_time() {
	let mut serializer = KeySerializer::new();
	let time = Time::from_hms(12, 30, 45).unwrap();
	serializer.extend_time(&time);
	let result = serializer.finish();
	assert_eq!(result.len(), 8);
}

#[test]
fn test_interval() {
	let mut serializer = KeySerializer::new();
	let duration = Duration::from_nanoseconds(1000000).unwrap();
	serializer.extend_duration(&duration);
	let result = serializer.finish();
	assert_eq!(result.len(), 16);
}

#[test]
fn test_row_number() {
	let mut serializer = KeySerializer::new();
	let row_number = RowNumber(42);
	serializer.extend_row_number(&row_number);
	let result = serializer.finish();
	assert_eq!(result.len(), 8);
}

#[test]
fn test_identity_id() {
	let (_, clock, rng) = test_clock_and_rng();
	let mut serializer = KeySerializer::new();
	let id = IdentityId::generate(&clock, &rng);
	serializer.extend_identity_id(&id);
	let result = serializer.finish();
	assert!(result.len() > 0);
}

#[test]
fn test_uuid4() {
	let mut serializer = KeySerializer::new();
	let uuid = Uuid4::generate();
	serializer.extend_uuid4(&uuid);
	let result = serializer.finish();
	assert!(result.len() > 16);
}

#[test]
fn test_uuid7() {
	let (_, clock, rng) = test_clock_and_rng();
	let mut serializer = KeySerializer::new();
	let uuid = Uuid7::generate(&clock, &rng);
	serializer.extend_uuid7(&uuid);
	let result = serializer.finish();
	assert!(result.len() > 16);
}

#[test]
fn test_blob() {
	let mut serializer = KeySerializer::new();
	let blob = Blob::from(vec![0x01, 0x02, 0x03]);
	serializer.extend_blob(&blob);
	let result = serializer.finish();
	assert!(result.len() > 3);
}

#[test]
fn test_int() {
	let mut serializer = KeySerializer::new();
	let int = Int(BigInt::from(42));
	serializer.extend_int(&int);
	let result = serializer.finish();
	assert!(result.len() > 0);
}

#[test]
fn test_uint() {
	let mut serializer = KeySerializer::new();
	let uint = Uint(BigInt::from(42));
	serializer.extend_uint(&uint);
	let result = serializer.finish();
	assert!(result.len() > 0);
}

#[test]
fn test_decimal() {
	let mut serializer = KeySerializer::new();
	let decimal = Decimal::from_str("3.14").unwrap();
	serializer.extend_decimal(&decimal);
	let result = serializer.finish();
	assert!(result.len() > 0);
}

#[test]
fn test_extend_value() {
	// Every value carries its type tag first, so a key can be decoded without a schema.
	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::none());
	let result = serializer.finish();
	assert_eq!(result, vec![0x00, 0x1a]); // none marker + Any type tag (ValueKind::Any = 26)

	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::none_of(ValueType::Int4));
	let result = serializer.finish();
	assert_eq!(result, vec![0x00, 0x06]); // marker + Int4 inner type marker

	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::Boolean(true));
	let result = serializer.finish();
	assert_eq!(result[0], 0x01); // Boolean marker
	assert_eq!(result.len(), 2); // marker + encoded bool

	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::Int4(42));
	let result = serializer.finish();
	assert_eq!(result[0], 0x06); // Int4 marker
	assert_eq!(result.len(), 5); // marker + 4 bytes

	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::Utf8("test".to_string()));
	let result = serializer.finish();
	assert_eq!(result[0], 0x09); // Utf8 marker
	assert!(result.ends_with(&[0xff, 0xff]));
}

#[test]
fn test_roundtrip_none() {
	let value = Value::none();
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_none_typed() {
	let value = Value::none_of(ValueType::Int4);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_boolean_true() {
	let value = Value::Boolean(true);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_boolean_false() {
	let value = Value::Boolean(false);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_float4() {
	let value = Value::Float4(OrderedF32::try_from(3.14f32).unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_float8() {
	let value = Value::Float8(OrderedF64::try_from(3.14).unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int1() {
	let value = Value::Int1(-42);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int2() {
	let value = Value::Int2(-1000);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int4() {
	let value = Value::Int4(42);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int8() {
	let value = Value::Int8(-1_000_000);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int16() {
	let value = Value::Int16(123_456_789);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_utf8() {
	let value = Value::Utf8("hello world".to_string());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint1() {
	let value = Value::Uint1(255);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint2() {
	let value = Value::Uint2(65535);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint4() {
	let value = Value::Uint4(100_000);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint8() {
	let value = Value::Uint8(999);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint16() {
	let value = Value::Uint16(u128::MAX);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_date() {
	let value = Value::Date(Date::from_ymd(2024, 6, 15).unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_datetime() {
	let value = Value::DateTime(DateTime::from_ymd_hms(2024, 6, 15, 12, 30, 45).unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_time() {
	let value = Value::Time(Time::from_hms(12, 30, 45).unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_duration() {
	let value = Value::duration_nanoseconds(1_000_000);
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_identity_id() {
	let (_, clock, rng) = test_clock_and_rng();
	let value = Value::IdentityId(IdentityId::generate(&clock, &rng));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uuid4() {
	let value = Value::Uuid4(Uuid4::generate());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uuid7() {
	let (_, clock, rng) = test_clock_and_rng();
	let value = Value::Uuid7(Uuid7::generate(&clock, &rng));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_blob() {
	let value = Value::Blob(Blob::from(vec![0x01, 0x02, 0x03]));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_int() {
	let value = Value::Int(Int(BigInt::from(-42)));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_uint() {
	let value = Value::Uint(Uint(BigInt::from(42)));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_decimal() {
	let value = Value::Decimal(Decimal::from_str("3.14").unwrap());
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_dictionary_id_u1() {
	let value = Value::DictionaryId(DictionaryEntryId::U1(42));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_dictionary_id_u2() {
	let value = Value::DictionaryId(DictionaryEntryId::U2(1000));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_dictionary_id_u4() {
	let value = Value::DictionaryId(DictionaryEntryId::U4(100_000));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_dictionary_id_u8() {
	let value = Value::DictionaryId(DictionaryEntryId::U8(10_000_000_000));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_dictionary_id_u16() {
	let value = Value::DictionaryId(DictionaryEntryId::U16(u128::MAX));
	let mut ser = KeySerializer::new();
	ser.extend_value(&value);
	let bytes = ser.finish();
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), value);
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_all() {
	let (_, clock, rng) = test_clock_and_rng();
	let values = vec![
		Value::none(),
		Value::none_of(ValueType::Int4),
		Value::Boolean(true),
		Value::Boolean(false),
		Value::Float4(OrderedF32::try_from(3.14f32).unwrap()),
		Value::Float8(OrderedF64::try_from(3.14).unwrap()),
		Value::Int1(-42),
		Value::Int2(-1000),
		Value::Int4(42),
		Value::Int8(-1_000_000),
		Value::Int16(123_456_789),
		Value::Utf8("hello world".to_string()),
		Value::Uint1(255),
		Value::Uint2(65535),
		Value::Uint4(100_000),
		Value::Uint8(999),
		Value::Uint16(u128::MAX),
		Value::Date(Date::from_ymd(2024, 6, 15).unwrap()),
		Value::DateTime(DateTime::from_ymd_hms(2024, 6, 15, 12, 30, 45).unwrap()),
		Value::Time(Time::from_hms(12, 30, 45).unwrap()),
		Value::duration_nanoseconds(1_000_000),
		Value::IdentityId(IdentityId::generate(&clock, &rng)),
		Value::Uuid4(Uuid4::generate()),
		Value::Uuid7(Uuid7::generate(&clock, &rng)),
		Value::Blob(Blob::from(vec![0x01, 0x02, 0x03])),
		Value::Int(Int(BigInt::from(-42))),
		Value::Uint(Uint(BigInt::from(42))),
		Value::Decimal(Decimal::from_str("3.14").unwrap()),
		Value::DictionaryId(DictionaryEntryId::U8(42)),
	];

	let mut ser = KeySerializer::new();
	for v in &values {
		ser.extend_value(v);
	}
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	for expected in &values {
		let actual = de.read_value().unwrap();
		assert_eq!(&actual, expected);
	}
	assert!(de.is_empty());
}

#[test]
fn test_roundtrip_exhaustiveness_guard() {
	// A new Value variant stops this compiling, forcing a round-trip test for it above.
	let value = Value::none();
	match value {
		Value::None {
			..
		} => {}
		Value::Boolean(_) => {}
		Value::Float4(_) => {}
		Value::Float8(_) => {}
		Value::Int1(_) => {}
		Value::Int2(_) => {}
		Value::Int4(_) => {}
		Value::Int8(_) => {}
		Value::Int16(_) => {}
		Value::Utf8(_) => {}
		Value::Uint1(_) => {}
		Value::Uint2(_) => {}
		Value::Uint4(_) => {}
		Value::Uint8(_) => {}
		Value::Uint16(_) => {}
		Value::Date(_) => {}
		Value::DateTime(_) => {}
		Value::Time(_) => {}
		Value::Duration(_) => {}
		Value::IdentityId(_) => {}
		Value::Uuid4(_) => {}
		Value::Uuid7(_) => {}
		Value::Blob(_) => {}
		Value::Int(_) => {}
		Value::Uint(_) => {}
		Value::Decimal(_) => {}
		Value::DictionaryId(_) => {}
		// Not serializable in keys:
		Value::Any(_) => {}
		Value::Type(_) => {}
		Value::List(_) => {}
		Value::Record(_) => {}
		Value::Tuple(_) => {}
	}
}

#[test]
fn test_to_encoded_key() {
	let mut serializer = KeySerializer::new();
	serializer.extend_i32(42);
	let key = serializer.to_encoded_key();
	assert_eq!(key.len(), 4);
}
