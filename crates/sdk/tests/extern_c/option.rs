// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
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
fn dt(nanos: i64) -> DateTime {
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

#[test]
fn option_bool_all_defined() {
	let input = factory::bool_with_bitvec("c", [true, false, true], vec![true, true, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_all_defined", &input, &output);
}

#[test]
fn option_bool_all_undefined() {
	let input = factory::bool_with_bitvec("c", vec![false; 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_all_undefined", &input, &output);
}

#[test]
fn option_bool_alternating_eight_rows() {
	let input = factory::bool_with_bitvec(
		"c",
		[true, false, false, false, true, false, false, false],
		vec![true, false, true, false, true, false, true, false],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_8", &input, &output);
}

#[test]
fn option_bool_alternating_nine_rows() {
	let input = factory::bool_with_bitvec(
		"c",
		[true, false, false, false, true, false, false, false, true],
		vec![true, false, true, false, true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_9", &input, &output);
}

#[test]
fn option_bool_first_undefined() {
	let input = factory::bool_with_bitvec("c", [false, true, false], vec![false, true, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_first_undef", &input, &output);
}

#[test]
fn option_float4_alternating() {
	let input = factory::float4_with_bitvec(
		"c",
		[1.5f32, 0.0, f32::INFINITY, 0.0, f32::NEG_INFINITY],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_alt", &input, &output);
}

#[test]
fn option_float4_all_undefined() {
	let input = factory::float4_with_bitvec("c", vec![0.0; 5], vec![false; 5]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_all_undef", &input, &output);
}

#[test]
fn option_float4_first_undefined() {
	let input = factory::float4_with_bitvec("c", [0.0, f32::MIN, f32::MAX], vec![false, true, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float4_first_undef", &input, &output);
}

#[test]
fn option_float8_alternating() {
	let input = factory::float8_with_bitvec(
		"c",
		[1.5f64, 0.0, f64::INFINITY, 0.0, -0.0f64],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float8_alt", &input, &output);
}

#[test]
fn option_float8_all_undefined() {
	let input = factory::float8_with_bitvec("c", vec![0.0; 5], vec![false; 5]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_float8_all_undef", &input, &output);
}

#[test]
fn option_int1_alternating() {
	let input = factory::int1_with_bitvec("c", [i8::MIN, 0, 0i8, 0, i8::MAX], vec![true, false, true, false, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int1_alt", &input, &output);
}

#[test]
fn option_int1_all_undefined() {
	let input = factory::int1_with_bitvec("c", vec![0; 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int1_all_undef", &input, &output);
}

#[test]
fn option_int2_alternating() {
	let input =
		factory::int2_with_bitvec("c", [i16::MIN, 0, 0i16, 0, i16::MAX], vec![true, false, true, false, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int2_alt", &input, &output);
}

#[test]
fn option_int4_alternating() {
	let input = factory::int4_optional("c", [Some(i32::MIN), None, Some(0i32), None, Some(i32::MAX)]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int4_alt", &input, &output);
}

#[test]
fn option_int8_all_defined() {
	let input = factory::int8_with_bitvec("c", [1i64, 2i64, 3i64], vec![true, true, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_all_defined", &input, &output);
}

#[test]
fn option_int8_all_undefined() {
	let input = factory::int8_with_bitvec("c", vec![0; 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_all_undefined", &input, &output);
}

#[test]
fn option_int8_alternating_eight_rows() {
	let input = factory::int8_with_bitvec(
		"c",
		[1i64, 0, 2i64, 0, 3i64, 0, 4i64, 0],
		vec![true, false, true, false, true, false, true, false],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_alt_8", &input, &output);
}

#[test]
fn option_int8_alternating_nine_rows() {
	let input = factory::int8_with_bitvec(
		"c",
		[1i64, 0, 2i64, 0, 3i64, 0, 4i64, 0, 5i64],
		vec![true, false, true, false, true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_alt_9", &input, &output);
}

#[test]
fn option_int16_alternating() {
	let input = factory::int16_with_bitvec(
		"c",
		[i128::MIN, 0, 0i128, 0, i128::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int16_alt", &input, &output);
}

#[test]
fn option_uint1_alternating() {
	let input = factory::uint1_with_bitvec("c", [0u8, 0, 127u8, 0, u8::MAX], vec![true, false, true, false, true]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint1_alt", &input, &output);
}

#[test]
fn option_uint2_alternating() {
	let input = factory::uint2_with_bitvec(
		"c",
		[0u16, 0, 32_768u16, 0, u16::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint2_alt", &input, &output);
}

#[test]
fn option_uint4_alternating() {
	let input = factory::uint4_with_bitvec(
		"c",
		[0u32, 0, 0x8000_0000u32, 0, u32::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint4_alt", &input, &output);
}

#[test]
fn option_uint8_alternating() {
	let input = factory::uint8_with_bitvec(
		"c",
		[0u64, 0, 0x8000_0000_0000_0000u64, 0, u64::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint8_alt", &input, &output);
}

#[test]
fn option_uint16_alternating() {
	let input = factory::uint16_with_bitvec(
		"c",
		[0u128, 0, 1u128 << 100, 0, u128::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uint16_alt", &input, &output);
}

#[test]
fn option_utf8_all_defined() {
	let input = factory::utf8_with_bitvec(
		"c",
		["a".to_string(), "bb".to_string(), "ccc".to_string()],
		vec![true, true, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_all_defined", &input, &output);
}

#[test]
fn option_utf8_all_undefined() {
	let input = factory::utf8_with_bitvec("c", vec![String::new(); 3], vec![false; 3]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_all_undef", &input, &output);
}

#[test]
fn option_utf8_first_undefined() {
	let input = factory::utf8_with_bitvec(
		"c",
		[String::new(), "hello".to_string(), "world".to_string()],
		vec![false, true, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_first_undef", &input, &output);
}

#[test]
fn option_utf8_last_undefined() {
	let input = factory::utf8_with_bitvec(
		"c",
		["hello".to_string(), "world".to_string(), String::new()],
		vec![true, true, false],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_last_undef", &input, &output);
}

#[test]
fn option_utf8_with_empty_string() {
	let input = factory::utf8_with_bitvec(
		"c",
		[String::new(), String::new(), "hello".to_string()],
		vec![true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_empty_string", &input, &output);
}

#[test]
fn option_utf8_alternating_eight_rows() {
	let input = factory::utf8_with_bitvec(
		"c",
		[
			"a".to_string(),
			String::new(),
			"bb".to_string(),
			String::new(),
			"ccc".to_string(),
			String::new(),
			"dddd".to_string(),
			String::new(),
		],
		vec![true, false, true, false, true, false, true, false],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_alt_8", &input, &output);
}

#[test]
fn option_utf8_alternating_nine_rows() {
	let input = factory::utf8_with_bitvec(
		"c",
		[
			"a".to_string(),
			String::new(),
			"bb".to_string(),
			String::new(),
			"ccc".to_string(),
			String::new(),
			"dddd".to_string(),
			String::new(),
			"eeeee".to_string(),
		],
		vec![true, false, true, false, true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_utf8_alt_9", &input, &output);
}

#[test]
fn option_blob_alternating() {
	let input = factory::blob_with_bitvec(
		"c",
		[
			Blob::new(vec![0x01]),
			Blob::default(),
			Blob::new(vec![0x02, 0x03]),
			Blob::default(),
			Blob::new(vec![]),
		],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_alt", &input, &output);
}

#[test]
fn option_blob_all_undefined() {
	let input = factory::blob_with_bitvec("c", vec![Blob::default(); 3], vec![false; 3]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_all_undef", &input, &output);
}

#[test]
fn option_blob_first_undefined() {
	let input = factory::blob_with_bitvec(
		"c",
		[Blob::default(), Blob::new(vec![0x01, 0x02]), Blob::new(vec![0x03])],
		vec![false, true, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_blob_first_undef", &input, &output);
}

#[test]
fn option_date_alternating() {
	let input = factory::date_with_bitvec(
		"c",
		[date_d(0), Date::default(), Date::from_ymd(2024, 2, 29).unwrap(), Date::default(), date_d(-365 * 100)],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_date_alt", &input, &output);
}

#[test]
fn option_date_all_undefined() {
	let input = factory::date_with_bitvec("c", vec![Date::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_date_all_undef", &input, &output);
}

#[test]
fn option_datetime_alternating() {
	let input = factory::datetime_with_bitvec(
		"c",
		[dt(0), DateTime::default(), dt(1), DateTime::default(), dt(i64::MAX)],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_datetime_alt", &input, &output);
}

#[test]
fn option_datetime_all_undefined() {
	let input = factory::datetime_with_bitvec("c", vec![DateTime::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_datetime_all_undef", &input, &output);
}

#[test]
fn option_time_alternating() {
	let input = factory::time_with_bitvec(
		"c",
		[t(0), Time::default(), t(1), Time::default(), Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_time_alt", &input, &output);
}

#[test]
fn option_time_all_undefined() {
	let input = factory::time_with_bitvec("c", vec![Time::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_time_all_undef", &input, &output);
}

#[test]
fn option_duration_alternating() {
	let input = factory::duration_with_bitvec(
		"c",
		[
			dur(0, 0, 0),
			Duration::default(),
			dur(12, 31, 1_000_000_000),
			Duration::default(),
			dur(-3, -7, -1_500_000_000),
		],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_duration_alt", &input, &output);
}

#[test]
fn option_duration_all_undefined() {
	let input = factory::duration_with_bitvec("c", vec![Duration::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_duration_all_undef", &input, &output);
}

#[test]
fn option_identity_id_alternating() {
	let input = factory::identity_id_with_bitvec(
		"c",
		[
			IdentityId::root(),
			IdentityId::default(),
			IdentityId::system(),
			IdentityId::default(),
			IdentityId::anonymous(),
		],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_alt", &input, &output);
}

#[test]
fn option_identity_id_all_undefined() {
	let input = factory::identity_id_with_bitvec("c", vec![IdentityId::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_all_undef", &input, &output);
}

#[test]
fn option_identity_id_first_undefined() {
	let input = factory::identity_id_with_bitvec(
		"c",
		[IdentityId::default(), IdentityId::root(), ident([0x01; 16])],
		vec![false, true, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_identity_first_undef", &input, &output);
}

#[test]
fn option_uuid4_alternating() {
	let input = factory::uuid4_with_bitvec(
		"c",
		[Uuid4(Uuid::nil()), Uuid4::default(), u4([0xAA; 16]), Uuid4::default(), u4([0x55; 16])],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid4_alt", &input, &output);
}

#[test]
fn option_uuid4_all_undefined() {
	let input = factory::uuid4_with_bitvec("c", vec![Uuid4::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid4_all_undef", &input, &output);
}

#[test]
fn option_uuid7_alternating() {
	let input = factory::uuid7_with_bitvec(
		"c",
		[Uuid7(Uuid::nil()), Uuid7::default(), u7([0xAA; 16]), Uuid7::default(), u7([0x55; 16])],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid7_alt", &input, &output);
}

#[test]
fn option_uuid7_all_undefined() {
	let input = factory::uuid7_with_bitvec("c", vec![Uuid7::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_uuid7_all_undef", &input, &output);
}

#[test]
fn option_decimal_alternating() {
	use std::str::FromStr;
	let input = factory::decimal_with_bitvec(
		"c",
		Precision::MAX,
		Scale::new(14),
		[
			Decimal::zero(),
			Decimal::default(),
			Decimal::from_i64(42),
			Decimal::default(),
			Decimal::from_str("3.14159265358979").unwrap(),
		],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_decimal_alt", &input, &output);
}

#[test]
fn option_decimal_all_undefined() {
	let input = factory::decimal_with_bitvec(
		"c",
		Precision::MAX,
		Scale::new(14),
		vec![Decimal::default(); 4],
		vec![false; 4],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_decimal_all_undef", &input, &output);
}

#[test]
fn option_any_alternating() {
	let input = factory::any_optional(
		"c",
		[Some(Value::Int8(7)), None, Some(Value::Utf8("x".to_string())), None, Some(Value::Boolean(true))],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_any_alt", &input, &output);
}

#[test]
fn option_any_all_undefined() {
	let input = factory::any_optional("c", vec![None; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_any_all_undef", &input, &output);
}

#[test]
fn option_dictionary_id_alternating() {
	let input = factory::dictionary_id_with_bitvec(
		"c",
		[
			DictionaryEntryId::U1(7),
			DictionaryEntryId::default(),
			DictionaryEntryId::U2(1234),
			DictionaryEntryId::default(),
			DictionaryEntryId::U16(u128::MAX),
		],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_alt", &input, &output);
}

#[test]
fn option_dictionary_id_all_undefined() {
	let input = factory::dictionary_id_with_bitvec("c", vec![DictionaryEntryId::default(); 4], vec![false; 4]);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_all_undef", &input, &output);
}

#[test]
fn option_dictionary_id_each_variant_with_undefined() {
	let input = factory::dictionary_id_with_bitvec(
		"c",
		[
			DictionaryEntryId::U1(7),
			DictionaryEntryId::default(),
			DictionaryEntryId::U2(1234),
			DictionaryEntryId::default(),
			DictionaryEntryId::U4(u32::MAX),
			DictionaryEntryId::default(),
			DictionaryEntryId::U8(u64::MAX),
			DictionaryEntryId::default(),
			DictionaryEntryId::U16(u128::MAX),
		],
		vec![true, false, true, false, true, false, true, false, true],
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_dict_id_each_variant", &input, &output);
}

#[test]
fn option_bool_alternating_sixteen_rows() {
	// 16 rows put the defined bitvec across two bytes, where a per-byte marshal loop would drop the second.
	let values: Vec<Option<bool>> = (0..16)
		.map(|i| {
			if i % 2 == 0 {
				Some(i % 4 == 0)
			} else {
				None
			}
		})
		.collect();
	let input = factory::bool_with_bitvec(
		"c",
		values.iter().map(|v| v.unwrap_or(false)),
		values.iter().map(Option::is_some).collect::<Vec<_>>(),
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_bool_sixteen", &input, &output);
}

#[test]
fn option_int8_thirty_two_rows_pattern() {
	// A numeric inner type exercises the bitvec at a power-of-two boundary independently of the bool path.
	let values: Vec<Option<i64>> = (0..32i64)
		.map(|i| {
			if i % 3 == 0 {
				None
			} else {
				Some(i)
			}
		})
		.collect();
	let input = factory::int8_with_bitvec(
		"c",
		values.iter().map(|v| v.unwrap_or(0)),
		values.iter().map(Option::is_some).collect::<Vec<_>>(),
	);
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
	let input = factory::int8_with_bitvec(
		"c",
		values.iter().map(|v| v.unwrap_or(0)),
		values.iter().map(Option::is_some).collect::<Vec<_>>(),
	);
	let output = round_trip_column("o", input.clone());
	assert_column_eq("option_int8_sixty_four", &input, &output);
}

#[allow(dead_code)]
fn _assert_helpers_used() {
	let _ = f32o(0.0);
	let _ = f64o(0.0);
}
