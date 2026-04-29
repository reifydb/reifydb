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
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};
use uuid::Uuid;

use super::common::{assert_column_eq, round_trip_column};

fn f32o(v: f32) -> OrderedF32 {
	OrderedF32::try_from(v).expect("rejected")
}
fn f64o(v: f64) -> OrderedF64 {
	OrderedF64::try_from(v).expect("rejected")
}
fn dt(nanos: u64) -> DateTime {
	DateTime::from_nanos(nanos)
}
fn date_d(days: i32) -> Date {
	Date::from_days_since_epoch(days).expect("valid")
}
fn t(nanos: u64) -> Time {
	Time::from_nanos_since_midnight(nanos).expect("valid")
}
fn dur(months: i32, days: i32, nanos: i64) -> Duration {
	Duration::new(months, days, nanos).expect("valid")
}
fn u4(b: [u8; 16]) -> Uuid4 {
	Uuid4(Uuid::from_bytes(b))
}
fn u7(b: [u8; 16]) -> Uuid7 {
	Uuid7(Uuid::from_bytes(b))
}
fn ident(b: [u8; 16]) -> IdentityId {
	IdentityId::new(Uuid7(Uuid::from_bytes(b)))
}

// Bool.
#[test]
fn option_bool_all_defined() {
	let input = ColumnBuffer::bool_optional([Some(true), Some(false), Some(true)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_all_defined", &input, &output);
}

#[test]
fn option_bool_all_undefined() {
	let nones: Vec<Option<bool>> = vec![None, None, None, None];
	let input = ColumnBuffer::bool_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_all_undefined", &input, &output);
}

#[test]
fn option_bool_alternating_eight_rows() {
	let input =
		ColumnBuffer::bool_optional([Some(true), None, Some(false), None, Some(true), None, Some(false), None]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_8", &input, &output);
}

#[test]
fn option_bool_alternating_nine_rows() {
	let input = ColumnBuffer::bool_optional([
		Some(true),
		None,
		Some(false),
		None,
		Some(true),
		None,
		Some(false),
		None,
		Some(true),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_9", &input, &output);
}

#[test]
fn option_bool_first_undefined() {
	let input = ColumnBuffer::bool_optional([None, Some(true), Some(false)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_first_undef", &input, &output);
}

// Float4.
#[test]
fn option_float4_alternating() {
	let input =
		ColumnBuffer::float4_optional([Some(1.5f32), None, Some(f32::INFINITY), None, Some(f32::NEG_INFINITY)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_alt", &input, &output);
}

#[test]
fn option_float4_all_undefined() {
	let nones: Vec<Option<f32>> = vec![None; 5];
	let input = ColumnBuffer::float4_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_all_undef", &input, &output);
}

#[test]
fn option_float4_first_undefined() {
	let input = ColumnBuffer::float4_optional([None, Some(f32::MIN), Some(f32::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_first_undef", &input, &output);
}

// Float8.
#[test]
fn option_float8_alternating() {
	let input = ColumnBuffer::float8_optional([Some(1.5f64), None, Some(f64::INFINITY), None, Some(-0.0f64)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float8_alt", &input, &output);
}

#[test]
fn option_float8_all_undefined() {
	let nones: Vec<Option<f64>> = vec![None; 5];
	let input = ColumnBuffer::float8_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float8_all_undef", &input, &output);
}

// Int1..Int16.
#[test]
fn option_int1_alternating() {
	let input = ColumnBuffer::int1_optional([Some(i8::MIN), None, Some(0i8), None, Some(i8::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int1_alt", &input, &output);
}

#[test]
fn option_int1_all_undefined() {
	let nones: Vec<Option<i8>> = vec![None; 4];
	let input = ColumnBuffer::int1_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int1_all_undef", &input, &output);
}

#[test]
fn option_int2_alternating() {
	let input = ColumnBuffer::int2_optional([Some(i16::MIN), None, Some(0i16), None, Some(i16::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int2_alt", &input, &output);
}

#[test]
fn option_int4_alternating() {
	let input = ColumnBuffer::int4_optional([Some(i32::MIN), None, Some(0i32), None, Some(i32::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int4_alt", &input, &output);
}

#[test]
fn option_int8_all_defined() {
	let input = ColumnBuffer::int8_optional([Some(1i64), Some(2i64), Some(3i64)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_all_defined", &input, &output);
}

#[test]
fn option_int8_all_undefined() {
	let nones: Vec<Option<i64>> = vec![None, None, None, None];
	let input = ColumnBuffer::int8_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_all_undefined", &input, &output);
}

#[test]
fn option_int8_alternating_eight_rows() {
	let input =
		ColumnBuffer::int8_optional([Some(1i64), None, Some(2i64), None, Some(3i64), None, Some(4i64), None]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_alt_8", &input, &output);
}

#[test]
fn option_int8_alternating_nine_rows() {
	let input = ColumnBuffer::int8_optional([
		Some(1i64),
		None,
		Some(2i64),
		None,
		Some(3i64),
		None,
		Some(4i64),
		None,
		Some(5i64),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_alt_9", &input, &output);
}

#[test]
fn option_int16_alternating() {
	let input = ColumnBuffer::int16_optional([Some(i128::MIN), None, Some(0i128), None, Some(i128::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int16_alt", &input, &output);
}

// Uint1..Uint16.
#[test]
fn option_uint1_alternating() {
	let input = ColumnBuffer::uint1_optional([Some(0u8), None, Some(127u8), None, Some(u8::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint1_alt", &input, &output);
}

#[test]
fn option_uint2_alternating() {
	let input = ColumnBuffer::uint2_optional([Some(0u16), None, Some(32_768u16), None, Some(u16::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint2_alt", &input, &output);
}

#[test]
fn option_uint4_alternating() {
	let input = ColumnBuffer::uint4_optional([Some(0u32), None, Some(0x8000_0000u32), None, Some(u32::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint4_alt", &input, &output);
}

#[test]
fn option_uint8_alternating() {
	let input =
		ColumnBuffer::uint8_optional([Some(0u64), None, Some(0x8000_0000_0000_0000u64), None, Some(u64::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint8_alt", &input, &output);
}

#[test]
fn option_uint16_alternating() {
	let input = ColumnBuffer::uint16_optional([Some(0u128), None, Some(1u128 << 100), None, Some(u128::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint16_alt", &input, &output);
}

// Utf8.
#[test]
fn option_utf8_all_defined() {
	let input =
		ColumnBuffer::utf8_optional([Some("a".to_string()), Some("bb".to_string()), Some("ccc".to_string())]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_all_defined", &input, &output);
}

#[test]
fn option_utf8_all_undefined() {
	let nones: Vec<Option<String>> = vec![None, None, None];
	let input = ColumnBuffer::utf8_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_all_undef", &input, &output);
}

#[test]
fn option_utf8_first_undefined() {
	let input = ColumnBuffer::utf8_optional([None, Some("hello".to_string()), Some("world".to_string())]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_first_undef", &input, &output);
}

#[test]
fn option_utf8_last_undefined() {
	let input = ColumnBuffer::utf8_optional([Some("hello".to_string()), Some("world".to_string()), None]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_last_undef", &input, &output);
}

#[test]
fn option_utf8_with_empty_string() {
	let input = ColumnBuffer::utf8_optional([Some(String::new()), None, Some("hello".to_string())]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_empty_string", &input, &output);
}

#[test]
fn option_utf8_alternating_eight_rows() {
	let input = ColumnBuffer::utf8_optional([
		Some("a".to_string()),
		None,
		Some("bb".to_string()),
		None,
		Some("ccc".to_string()),
		None,
		Some("dddd".to_string()),
		None,
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_alt_8", &input, &output);
}

#[test]
fn option_utf8_alternating_nine_rows() {
	let input = ColumnBuffer::utf8_optional([
		Some("a".to_string()),
		None,
		Some("bb".to_string()),
		None,
		Some("ccc".to_string()),
		None,
		Some("dddd".to_string()),
		None,
		Some("eeeee".to_string()),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_alt_9", &input, &output);
}

// Blob.
#[test]
fn option_blob_alternating() {
	let input = ColumnBuffer::blob_optional([
		Some(Blob::new(vec![0x01])),
		None,
		Some(Blob::new(vec![0x02, 0x03])),
		None,
		Some(Blob::new(vec![])),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_alt", &input, &output);
}

#[test]
fn option_blob_all_undefined() {
	let nones: Vec<Option<Blob>> = vec![None, None, None];
	let input = ColumnBuffer::blob_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_all_undef", &input, &output);
}

#[test]
fn option_blob_first_undefined() {
	let input = ColumnBuffer::blob_optional([None, Some(Blob::new(vec![0x01, 0x02])), Some(Blob::new(vec![0x03]))]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_first_undef", &input, &output);
}

// Date.
#[test]
fn option_date_alternating() {
	let input = ColumnBuffer::date_optional([
		Some(date_d(0)),
		None,
		Some(Date::from_ymd(2024, 2, 29).unwrap()),
		None,
		Some(date_d(-365 * 100)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_date_alt", &input, &output);
}

#[test]
fn option_date_all_undefined() {
	let nones: Vec<Option<Date>> = vec![None; 4];
	let input = ColumnBuffer::date_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_date_all_undef", &input, &output);
}

// DateTime.
#[test]
fn option_datetime_alternating() {
	let input = ColumnBuffer::datetime_optional([Some(dt(0)), None, Some(dt(1)), None, Some(dt(u64::MAX))]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_datetime_alt", &input, &output);
}

#[test]
fn option_datetime_all_undefined() {
	let nones: Vec<Option<DateTime>> = vec![None; 4];
	let input = ColumnBuffer::datetime_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_datetime_all_undef", &input, &output);
}

// Time.
#[test]
fn option_time_alternating() {
	let input = ColumnBuffer::time_optional([
		Some(t(0)),
		None,
		Some(t(1)),
		None,
		Some(Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_time_alt", &input, &output);
}

#[test]
fn option_time_all_undefined() {
	let nones: Vec<Option<Time>> = vec![None; 4];
	let input = ColumnBuffer::time_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_time_all_undef", &input, &output);
}

// Duration.
#[test]
fn option_duration_alternating() {
	let input = ColumnBuffer::duration_optional([
		Some(dur(0, 0, 0)),
		None,
		Some(dur(12, 31, 1_000_000_000)),
		None,
		Some(dur(-3, -7, -1_500_000_000)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_duration_alt", &input, &output);
}

#[test]
fn option_duration_all_undefined() {
	let nones: Vec<Option<Duration>> = vec![None; 4];
	let input = ColumnBuffer::duration_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_duration_all_undef", &input, &output);
}

// IdentityId.
#[test]
fn option_identity_id_alternating() {
	let input = ColumnBuffer::identity_id_optional([
		Some(IdentityId::root()),
		None,
		Some(IdentityId::system()),
		None,
		Some(IdentityId::anonymous()),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_alt", &input, &output);
}

#[test]
fn option_identity_id_all_undefined() {
	let nones: Vec<Option<IdentityId>> = vec![None; 4];
	let input = ColumnBuffer::identity_id_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_all_undef", &input, &output);
}

#[test]
fn option_identity_id_first_undefined() {
	let input = ColumnBuffer::identity_id_optional([None, Some(IdentityId::root()), Some(ident([0x01; 16]))]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_first_undef", &input, &output);
}

// Uuid4.
#[test]
fn option_uuid4_alternating() {
	let input = ColumnBuffer::uuid4_optional([
		Some(Uuid4(Uuid::nil())),
		None,
		Some(u4([0xAA; 16])),
		None,
		Some(u4([0x55; 16])),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid4_alt", &input, &output);
}

#[test]
fn option_uuid4_all_undefined() {
	let nones: Vec<Option<Uuid4>> = vec![None; 4];
	let input = ColumnBuffer::uuid4_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid4_all_undef", &input, &output);
}

// Uuid7.
#[test]
fn option_uuid7_alternating() {
	let input = ColumnBuffer::uuid7_optional([
		Some(Uuid7(Uuid::nil())),
		None,
		Some(u7([0xAA; 16])),
		None,
		Some(u7([0x55; 16])),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid7_alt", &input, &output);
}

#[test]
fn option_uuid7_all_undefined() {
	let nones: Vec<Option<Uuid7>> = vec![None; 4];
	let input = ColumnBuffer::uuid7_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid7_all_undef", &input, &output);
}

// Int (BigInt).
#[test]
fn option_int_alternating() {
	let input = ColumnBuffer::int_optional([
		Some(Int::zero()),
		None,
		Some(Int::from_i64(42)),
		None,
		Some(Int::from_i128(i128::MAX)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int_alt", &input, &output);
}

#[test]
fn option_int_all_undefined() {
	let nones: Vec<Option<Int>> = vec![None; 4];
	let input = ColumnBuffer::int_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int_all_undef", &input, &output);
}

// Uint (BigInt).
#[test]
fn option_uint_alternating() {
	let input = ColumnBuffer::uint_optional([
		Some(Uint::zero()),
		None,
		Some(Uint::from_u64(42)),
		None,
		Some(Uint::from_u128(u128::MAX)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint_alt", &input, &output);
}

#[test]
fn option_uint_all_undefined() {
	let nones: Vec<Option<Uint>> = vec![None; 4];
	let input = ColumnBuffer::uint_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint_all_undef", &input, &output);
}

// Decimal.
#[test]
fn option_decimal_alternating() {
	use std::str::FromStr;
	let input = ColumnBuffer::decimal_optional([
		Some(Decimal::zero()),
		None,
		Some(Decimal::from_i64(42)),
		None,
		Some(Decimal::from_str("3.14159265358979").unwrap()),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_decimal_alt", &input, &output);
}

#[test]
fn option_decimal_all_undefined() {
	let nones: Vec<Option<Decimal>> = vec![None; 4];
	let input = ColumnBuffer::decimal_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_decimal_all_undef", &input, &output);
}

// Any.
#[test]
fn option_any_alternating() {
	let input = ColumnBuffer::any_optional([
		Some(Box::new(Value::Int8(7))),
		None,
		Some(Box::new(Value::Utf8("x".to_string()))),
		None,
		Some(Box::new(Value::Boolean(true))),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_any_alt", &input, &output);
}

#[test]
fn option_any_all_undefined() {
	let nones: Vec<Option<Box<Value>>> = vec![None; 4];
	let input = ColumnBuffer::any_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_any_all_undef", &input, &output);
}

// DictionaryId.
#[test]
fn option_dictionary_id_alternating() {
	let input = ColumnBuffer::dictionary_id_optional([
		Some(DictionaryEntryId::U1(7)),
		None,
		Some(DictionaryEntryId::U2(1234)),
		None,
		Some(DictionaryEntryId::U16(u128::MAX)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_alt", &input, &output);
}

#[test]
fn option_dictionary_id_all_undefined() {
	let nones: Vec<Option<DictionaryEntryId>> = vec![None; 4];
	let input = ColumnBuffer::dictionary_id_optional(nones);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_all_undef", &input, &output);
}

#[test]
fn option_dictionary_id_each_variant_with_undefined() {
	let input = ColumnBuffer::dictionary_id_optional([
		Some(DictionaryEntryId::U1(7)),
		None,
		Some(DictionaryEntryId::U2(1234)),
		None,
		Some(DictionaryEntryId::U4(u32::MAX)),
		None,
		Some(DictionaryEntryId::U8(u64::MAX)),
		None,
		Some(DictionaryEntryId::U16(u128::MAX)),
	]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_each_variant", &input, &output);
}

// 16-row bitvec spanning two bytes - the previous bool case kept for parity.
#[test]
fn option_bool_alternating_sixteen_rows() {
	let values: Vec<Option<bool>> = (0..16)
		.map(|i| {
			if i % 2 == 0 {
				Some(i % 4 == 0)
			} else {
				None
			}
		})
		.collect();
	let input = ColumnBuffer::bool_optional(values);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_sixteen", &input, &output);
}

// Bitvec power-of-two boundaries with a numeric inner type to cross-check
// the bitvec independent of the inner type's marshal path.
#[test]
fn option_int8_thirty_two_rows_pattern() {
	let values: Vec<Option<i64>> = (0..32i64)
		.map(|i| {
			if i % 3 == 0 {
				None
			} else {
				Some(i)
			}
		})
		.collect();
	let input = ColumnBuffer::int8_optional(values);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_thirty_two", &input, &output);
}

#[test]
fn option_int8_sixty_four_rows_pattern() {
	let values: Vec<Option<i64>> = (0..64i64)
		.map(|i| {
			if i % 5 == 0 {
				None
			} else {
				Some(i * 7)
			}
		})
		.collect();
	let input = ColumnBuffer::int8_optional(values);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_sixty_four", &input, &output);
}

// Suppress unused warnings for helpers not used in all builds.
#[allow(dead_code)]
fn _assert_helpers_used() {
	let _ = f32o(0.0);
	let _ = f64o(0.0);
}
