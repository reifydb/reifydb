// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::{
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
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};
use uuid::Uuid;

use super::common::{assert_column_eq, round_trip_column};

fn float4(v: f32) -> Value {
	Value::Float4(OrderedF32::try_from(v).expect("OrderedF32::try_from rejected value"))
}
fn float8(v: f64) -> Value {
	Value::Float8(OrderedF64::try_from(v).expect("OrderedF64::try_from rejected value"))
}
fn blob(bytes: &[u8]) -> Value {
	Value::Blob(Blob::new(bytes.to_vec()))
}
fn date_days(days: i32) -> Value {
	Value::Date(Date::from_days_since_epoch(days).expect("valid"))
}
fn datetime_nanos(nanos: u64) -> Value {
	Value::DateTime(DateTime::from_nanos(nanos))
}
fn time_nanos(nanos: u64) -> Value {
	Value::Time(Time::from_nanos_since_midnight(nanos).expect("valid"))
}
fn duration(months: i32, days: i32, nanos: i64) -> Value {
	Value::Duration(Duration::new(months, days, nanos).expect("valid"))
}
fn uuid4_bytes(bytes: [u8; 16]) -> Value {
	Value::Uuid4(Uuid4(Uuid::from_bytes(bytes)))
}
fn uuid7_bytes(bytes: [u8; 16]) -> Value {
	Value::Uuid7(Uuid7(Uuid::from_bytes(bytes)))
}
fn identity_bytes(bytes: [u8; 16]) -> Value {
	Value::IdentityId(IdentityId::new(Uuid7(Uuid::from_bytes(bytes))))
}
fn one_row(value: Value) -> ColumnBuffer {
	ColumnBuffer::any([Box::new(value)])
}

// None.
#[test]
fn any_none() {
	let input = ColumnBuffer::any([Box::new(Value::none())]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_none", &input, &output);
}

#[test]
fn any_none_of_int8() {
	let input = ColumnBuffer::any([Box::new(Value::none_of(Type::Int8))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_none_of_int8", &input, &output);
}

// Boolean.
#[test]
fn any_boolean_true() {
	let input = one_row(Value::Boolean(true));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_boolean_true", &input, &output);
}

#[test]
fn any_boolean_false() {
	let input = one_row(Value::Boolean(false));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_boolean_false", &input, &output);
}

// Float4.
#[test]
fn any_float4_zero() {
	let input = one_row(float4(0.0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float4_zero", &input, &output);
}

#[test]
fn any_float4_min_max() {
	let input = ColumnBuffer::any([Box::new(float4(f32::MIN)), Box::new(float4(f32::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float4_min_max", &input, &output);
}

#[test]
fn any_float4_infinities() {
	let input = ColumnBuffer::any([Box::new(float4(f32::INFINITY)), Box::new(float4(f32::NEG_INFINITY))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float4_infinities", &input, &output);
}

// Float8.
#[test]
fn any_float8_zero() {
	let input = one_row(float8(0.0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float8_zero", &input, &output);
}

#[test]
fn any_float8_min_max() {
	let input = ColumnBuffer::any([Box::new(float8(f64::MIN)), Box::new(float8(f64::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float8_min_max", &input, &output);
}

#[test]
fn any_float8_infinities() {
	let input = ColumnBuffer::any([Box::new(float8(f64::INFINITY)), Box::new(float8(f64::NEG_INFINITY))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float8_infinities", &input, &output);
}

#[test]
fn any_float8_subnormal() {
	let input = one_row(float8(f64::from_bits(1)));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_float8_subnormal", &input, &output);
}

// Signed integers.
#[test]
fn any_int1_extremes() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int1(i8::MIN)),
		Box::new(Value::Int1(-1)),
		Box::new(Value::Int1(0)),
		Box::new(Value::Int1(1)),
		Box::new(Value::Int1(i8::MAX)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int1", &input, &output);
}

#[test]
fn any_int2_extremes() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int2(i16::MIN)),
		Box::new(Value::Int2(0x0102)),
		Box::new(Value::Int2(i16::MAX)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int2", &input, &output);
}

#[test]
fn any_int4_extremes() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int4(i32::MIN)),
		Box::new(Value::Int4(0x01020304)),
		Box::new(Value::Int4(i32::MAX)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int4", &input, &output);
}

#[test]
fn any_int8_extremes() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int8(i64::MIN)),
		Box::new(Value::Int8(0x0102_0304_0506_0708i64)),
		Box::new(Value::Int8(i64::MAX)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int8", &input, &output);
}

#[test]
fn any_int16_extremes() {
	let v: i128 = (0x0102_0304_0506_0708i128) | ((0x090A_0B0C_0D0E_0F10i128) << 64);
	let input = ColumnBuffer::any([
		Box::new(Value::Int16(i128::MIN)),
		Box::new(Value::Int16(v)),
		Box::new(Value::Int16(i128::MAX)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int16", &input, &output);
}

// Unsigned integers.
#[test]
fn any_uint1_extremes() {
	let input = ColumnBuffer::any([Box::new(Value::Uint1(0)), Box::new(Value::Uint1(u8::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint1", &input, &output);
}

#[test]
fn any_uint2_extremes() {
	let input = ColumnBuffer::any([Box::new(Value::Uint2(0)), Box::new(Value::Uint2(u16::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint2", &input, &output);
}

#[test]
fn any_uint4_extremes() {
	let input = ColumnBuffer::any([Box::new(Value::Uint4(0)), Box::new(Value::Uint4(u32::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint4", &input, &output);
}

#[test]
fn any_uint8_extremes() {
	let input = ColumnBuffer::any([Box::new(Value::Uint8(0)), Box::new(Value::Uint8(u64::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint8", &input, &output);
}

#[test]
fn any_uint16_extremes() {
	let input = ColumnBuffer::any([Box::new(Value::Uint16(0)), Box::new(Value::Uint16(u128::MAX))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint16", &input, &output);
}

// Utf8.
#[test]
fn any_utf8_empty() {
	let input = one_row(Value::Utf8(String::new()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_utf8_empty", &input, &output);
}

#[test]
fn any_utf8_multibyte() {
	let input = one_row(Value::Utf8("cafe \u{2603} multibyte".to_string()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_utf8_multibyte", &input, &output);
}

#[test]
fn any_utf8_embedded_null() {
	let input = one_row(Value::Utf8("a\0b\0c".to_string()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_utf8_embedded_null", &input, &output);
}

#[test]
fn any_utf8_long_4kib() {
	let s: String = (0..4096).map(|i| ((i % 26) as u8 + b'a') as char).collect();
	let input = one_row(Value::Utf8(s));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_utf8_long_4kib", &input, &output);
}

// Blob.
#[test]
fn any_blob_empty() {
	let input = one_row(blob(&[]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_blob_empty", &input, &output);
}

#[test]
fn any_blob_full_byte_range() {
	let bytes: Vec<u8> = (0..=255u8).collect();
	let input = one_row(blob(&bytes));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_blob_full_range", &input, &output);
}

#[test]
fn any_blob_embedded_zeros() {
	let input = one_row(blob(&[0x00, 0xFF, 0x00, 0xAB, 0x00]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_blob_embedded_zeros", &input, &output);
}

// Date.
#[test]
fn any_date_epoch() {
	let input = one_row(date_days(0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_date_epoch", &input, &output);
}

#[test]
fn any_date_far_past() {
	let input = one_row(date_days(-365 * 100));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_date_far_past", &input, &output);
}

#[test]
fn any_date_leap_day() {
	let input = one_row(Value::Date(Date::from_ymd(2024, 2, 29).expect("valid")));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_date_leap_day", &input, &output);
}

#[test]
fn any_date_far_future() {
	let input = one_row(date_days(365 * 1000));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_date_far_future", &input, &output);
}

// DateTime.
#[test]
fn any_datetime_epoch() {
	let input = one_row(datetime_nanos(0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_datetime_epoch", &input, &output);
}

#[test]
fn any_datetime_one_nano() {
	let input = one_row(datetime_nanos(1));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_datetime_one_nano", &input, &output);
}

#[test]
fn any_datetime_max_u64() {
	let input = one_row(datetime_nanos(u64::MAX));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_datetime_max", &input, &output);
}

// Time.
#[test]
fn any_time_midnight() {
	let input = one_row(time_nanos(0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_time_midnight", &input, &output);
}

#[test]
fn any_time_one_nano_past_midnight() {
	let input = one_row(time_nanos(1));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_time_one_nano", &input, &output);
}

#[test]
fn any_time_just_before_midnight() {
	let input = one_row(Value::Time(Time::from_hms_nano(23, 59, 59, 999_999_999).expect("valid")));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_time_just_before_midnight", &input, &output);
}

// Duration.
#[test]
fn any_duration_zero() {
	let input = one_row(duration(0, 0, 0));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_duration_zero", &input, &output);
}

#[test]
fn any_duration_pure_components() {
	let input = ColumnBuffer::any([
		Box::new(duration(12, 0, 0)),
		Box::new(duration(0, 31, 0)),
		Box::new(duration(0, 0, 1_000_000_000)),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_duration_components", &input, &output);
}

#[test]
fn any_duration_negative() {
	let input = one_row(duration(-3, -7, -1_500_000_000));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_duration_negative", &input, &output);
}

// IdentityId.
#[test]
fn any_identity_id_anonymous() {
	let input = one_row(Value::IdentityId(IdentityId::anonymous()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_identity_anonymous", &input, &output);
}

#[test]
fn any_identity_id_root() {
	let input = one_row(Value::IdentityId(IdentityId::root()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_identity_root", &input, &output);
}

#[test]
fn any_identity_id_specific() {
	let bytes = [0x01, 0x8D, 0x5E, 0x30, 0x4B, 0x78, 0x7A, 0xBC, 0x91, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
	let input = one_row(identity_bytes(bytes));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_identity_specific", &input, &output);
}

// Uuid4.
#[test]
fn any_uuid4_nil() {
	let input = one_row(Value::Uuid4(Uuid4(Uuid::nil())));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uuid4_nil", &input, &output);
}

#[test]
fn any_uuid4_specific() {
	let input = one_row(uuid4_bytes([
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x47, 0x08, 0x89, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
	]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uuid4_specific", &input, &output);
}

// Uuid7.
#[test]
fn any_uuid7_nil() {
	let input = one_row(Value::Uuid7(Uuid7(Uuid::nil())));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uuid7_nil", &input, &output);
}

#[test]
fn any_uuid7_specific() {
	let input = one_row(uuid7_bytes([
		0x01, 0x8D, 0x5E, 0x30, 0x4B, 0x78, 0x7A, 0xBC, 0x91, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
	]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uuid7_specific", &input, &output);
}

// Int (BigInt).
#[test]
fn any_int_zero() {
	let input = one_row(Value::Int(Int::zero()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int_zero", &input, &output);
}

#[test]
fn any_int_small() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int(Int::from_i64(-1))),
		Box::new(Value::Int(Int::from_i64(0))),
		Box::new(Value::Int(Int::from_i64(42))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int_small", &input, &output);
}

#[test]
fn any_int_i128_extremes() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int(Int::from_i128(i128::MIN))),
		Box::new(Value::Int(Int::from_i128(i128::MAX))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int_i128_extremes", &input, &output);
}

#[test]
fn any_int_outside_i128_range() {
	let mut big = Int::from_i128(i128::MAX);
	big.0 += Int::from_i128(i128::MAX).0;
	let mut neg_big = Int::from_i128(i128::MIN);
	neg_big.0 += Int::from_i128(i128::MIN).0;
	let input = ColumnBuffer::any([Box::new(Value::Int(big)), Box::new(Value::Int(neg_big))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_int_outside_i128", &input, &output);
}

// Uint (BigInt).
#[test]
fn any_uint_zero() {
	let input = one_row(Value::Uint(Uint::zero()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint_zero", &input, &output);
}

#[test]
fn any_uint_u128_max() {
	let input = one_row(Value::Uint(Uint::from_u128(u128::MAX)));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint_u128_max", &input, &output);
}

#[test]
fn any_uint_outside_u128_range() {
	let mut big = Uint::from_u128(u128::MAX);
	big.0 += Uint::from_u128(u128::MAX).0;
	let input = one_row(Value::Uint(big));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_uint_outside_u128", &input, &output);
}

// Decimal.
#[test]
fn any_decimal_zero() {
	let input = one_row(Value::Decimal(Decimal::zero()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_decimal_zero", &input, &output);
}

#[test]
fn any_decimal_simple() {
	let input = ColumnBuffer::any([
		Box::new(Value::Decimal(Decimal::from_i64(1))),
		Box::new(Value::Decimal(Decimal::from_i64(-1))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_decimal_simple", &input, &output);
}

#[test]
fn any_decimal_high_precision() {
	use std::str::FromStr;
	let input = ColumnBuffer::any([
		Box::new(Value::Decimal(Decimal::from_str("3.14159265358979323846").expect("valid"))),
		Box::new(Value::Decimal(Decimal::from_str("99999999999999999999999999999999").expect("valid"))),
		Box::new(Value::Decimal(Decimal::from_str("-0.0000000000000000000000000000001").expect("valid"))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_decimal_high_precision", &input, &output);
}

// Any (recursive).
#[test]
fn any_recursive_one_level() {
	let input = one_row(Value::Any(Box::new(Value::Int8(42))));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_recursive_one_level", &input, &output);
}

#[test]
fn any_recursive_two_levels() {
	let input = one_row(Value::Any(Box::new(Value::Any(Box::new(Value::Utf8("inner".to_string()))))));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_recursive_two_levels", &input, &output);
}

// DictionaryId.
#[test]
fn any_dictionary_id_each_variant() {
	let input = ColumnBuffer::any([
		Box::new(Value::DictionaryId(DictionaryEntryId::U1(7))),
		Box::new(Value::DictionaryId(DictionaryEntryId::U2(1234))),
		Box::new(Value::DictionaryId(DictionaryEntryId::U4(123_456_789))),
		Box::new(Value::DictionaryId(DictionaryEntryId::U8(0xDEAD_BEEF_CAFE_BABEu64))),
		Box::new(Value::DictionaryId(DictionaryEntryId::U16(0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_dict_id_each_variant", &input, &output);
}

// Type (meta).
#[test]
fn any_type_simple() {
	let input = ColumnBuffer::any([Box::new(Value::Type(Type::Int8)), Box::new(Value::Type(Type::Utf8))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_type_simple", &input, &output);
}

#[test]
fn any_type_recursive_list() {
	let input = one_row(Value::Type(Type::List(Box::new(Type::Int8))));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_type_list", &input, &output);
}

#[test]
fn any_type_recursive_record() {
	let input =
		one_row(Value::Type(Type::Record(vec![("x".to_string(), Type::Int4), ("y".to_string(), Type::Utf8)])));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_type_record", &input, &output);
}

// List.
#[test]
fn any_list_empty() {
	let input = one_row(Value::List(Vec::new()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_list_empty", &input, &output);
}

#[test]
fn any_list_homogeneous() {
	let input = one_row(Value::List(vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_list_homogeneous", &input, &output);
}

#[test]
fn any_list_mixed_types() {
	let input = one_row(Value::List(vec![
		Value::Int8(1),
		Value::Utf8("two".to_string()),
		Value::Boolean(true),
		Value::none(),
	]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_list_mixed", &input, &output);
}

#[test]
fn any_list_nested() {
	let inner = Value::List(vec![Value::Int8(10), Value::Int8(20)]);
	let input = one_row(Value::List(vec![inner, Value::List(vec![Value::Int8(30)])]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_list_nested", &input, &output);
}

// Record.
#[test]
fn any_record_empty() {
	let input = one_row(Value::Record(Vec::new()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_record_empty", &input, &output);
}

#[test]
fn any_record_single_field() {
	let input = one_row(Value::Record(vec![("count".to_string(), Value::Int8(42))]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_record_single", &input, &output);
}

#[test]
fn any_record_multi_field_mixed() {
	let input = one_row(Value::Record(vec![
		("id".to_string(), Value::Int8(1)),
		("name".to_string(), Value::Utf8("alice".to_string())),
		("active".to_string(), Value::Boolean(true)),
		("score".to_string(), float8(3.14)),
	]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_record_mixed", &input, &output);
}

// Tuple.
#[test]
fn any_tuple_empty() {
	let input = one_row(Value::Tuple(Vec::new()));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_tuple_empty", &input, &output);
}

#[test]
fn any_tuple_pair() {
	let input = one_row(Value::Tuple(vec![Value::Int8(1), Value::Utf8("two".to_string())]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_tuple_pair", &input, &output);
}

#[test]
fn any_tuple_triple_mixed() {
	let input = one_row(Value::Tuple(vec![Value::Int8(1), Value::Utf8("two".to_string()), Value::Boolean(true)]));
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_tuple_triple", &input, &output);
}

// Original cases.
#[test]
fn any_single_int() {
	let input = ColumnBuffer::any([Box::new(Value::Int8(42i64))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_single_int", &input, &output);
}

#[test]
fn any_single_utf8() {
	let input = ColumnBuffer::any([Box::new(Value::Utf8("hello".to_string()))]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_single_utf8", &input, &output);
}

#[test]
fn any_heterogeneous_column() {
	let input = ColumnBuffer::any([
		Box::new(Value::Int8(1i64)),
		Box::new(Value::Utf8("two".to_string())),
		Box::new(Value::Boolean(true)),
		Box::new(Value::Float8(OrderedF64::try_from(3.14f64).expect("valid"))),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_heterogeneous", &input, &output);
}

#[test]
fn any_thirty_two_rows() {
	let values: Vec<Box<Value>> = (0..32i64).map(|i| Box::new(Value::Int8(i))).collect();
	let input = ColumnBuffer::any(values);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_thirty_two", &input, &output);
}

#[test]
fn any_with_undefined() {
	let input = ColumnBuffer::any_optional([
		Some(Box::new(Value::Int8(7i64))),
		None,
		Some(Box::new(Value::Utf8("x".to_string()))),
		None,
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_with_undefined", &input, &output);
}

// Big mixed column - if any single variant breaks the round trip, this fires.
#[test]
fn any_one_per_variant_in_one_column() {
	let input = ColumnBuffer::any([
		Box::new(Value::none()),
		Box::new(Value::Boolean(true)),
		Box::new(float4(1.5)),
		Box::new(float8(2.5)),
		Box::new(Value::Int1(-1)),
		Box::new(Value::Int2(-2)),
		Box::new(Value::Int4(-4)),
		Box::new(Value::Int8(-8)),
		Box::new(Value::Int16(-16)),
		Box::new(Value::Uint1(1)),
		Box::new(Value::Uint2(2)),
		Box::new(Value::Uint4(4)),
		Box::new(Value::Uint8(8)),
		Box::new(Value::Uint16(16)),
		Box::new(Value::Utf8("hello".to_string())),
		Box::new(blob(&[0x01, 0x02, 0x03])),
		Box::new(date_days(100)),
		Box::new(datetime_nanos(123_456_789)),
		Box::new(time_nanos(456_789)),
		Box::new(duration(1, 2, 3)),
		Box::new(Value::IdentityId(IdentityId::root())),
		Box::new(uuid4_bytes([0xAA; 16])),
		Box::new(uuid7_bytes([0x55; 16])),
		Box::new(Value::Int(Int::from_i64(7))),
		Box::new(Value::Uint(Uint::from_u64(7))),
		Box::new(Value::Decimal(Decimal::from_i64(7))),
		Box::new(Value::Any(Box::new(Value::Int8(42)))),
		Box::new(Value::DictionaryId(DictionaryEntryId::U4(7))),
		Box::new(Value::Type(Type::Int8)),
		Box::new(Value::List(vec![Value::Int8(1), Value::Int8(2)])),
		Box::new(Value::Record(vec![("k".to_string(), Value::Int8(1))])),
		Box::new(Value::Tuple(vec![Value::Int8(1), Value::Boolean(false)])),
	]);
	let output = round_trip_column("a", input.clone());
	assert_column_eq("any_one_per_variant", &input, &output);
}
