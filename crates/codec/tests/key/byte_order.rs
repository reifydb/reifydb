// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use num_bigint::BigInt;
use reifydb_codec::key::{deserializer::KeyDeserializer, encode_bytes, serializer::KeySerializer, sort::SortOrder};
use reifydb_value::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
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

fn enc(s: &str) -> Vec<u8> {
	let mut out = Vec::new();
	encode_bytes(s.as_bytes(), &mut out);
	out
}

#[test]
fn byte_content_sorts_descending_like_every_other_key_column() {
	// a sorted view does no read-time sort, so this order is what the user sees.
	assert!(enc("ab") < enc("aa"), "the greater string must encode smaller");
	assert!(enc("b") < enc("a"), "the greater string must encode smaller");
}

#[test]
fn a_prefix_sorts_after_the_string_that_extends_it() {
	// descending puts the longer string first; the terminator must outrank any content byte.
	assert!(enc("ab") < enc("a"));
	assert!(enc("a") < enc(""));
}

#[test]
fn a_zero_byte_sorts_last_within_a_position() {
	// 0x00 is the smallest content value, so under descending it must encode largest.
	let zero = {
		let mut out = Vec::new();
		encode_bytes(&[0x00], &mut out);
		out
	};
	let one = {
		let mut out = Vec::new();
		encode_bytes(&[0x01], &mut out);
		out
	};
	assert!(one < zero, "0x01 is greater than 0x00 so it must encode smaller");
}

fn enc_int(v: i64) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_int(&Int(BigInt::from(v)));
	s.to_encoded_key().to_vec()
}

fn enc_uint(v: u64) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_uint(&Uint(BigInt::from(v)));
	s.to_encoded_key().to_vec()
}

fn enc_dec(s: &str) -> Vec<u8> {
	let mut ser = KeySerializer::new();
	ser.extend_decimal(&Decimal::from_str(s).unwrap());
	ser.to_encoded_key().to_vec()
}

#[test]
fn uint_sorts_descending_by_value() {
	// a uint key column must order like every other numeric column or range scans skip rows.
	assert!(enc_uint(2) < enc_uint(1));
	assert!(enc_uint(256) < enc_uint(255), "must hold across a magnitude-length boundary");
	assert!(enc_uint(1) < enc_uint(0));
}

#[test]
fn int_sorts_descending_by_value() {
	// negatives must trail positives; a wrong sign byte silently reverses half the range.
	assert!(enc_int(1) < enc_int(0));
	assert!(enc_int(0) < enc_int(-1));
	assert!(enc_int(-1) < enc_int(-2));
	assert!(enc_int(-255) < enc_int(-256), "must hold across a magnitude-length boundary");
	assert!(enc_int(256) < enc_int(255));
}

#[test]
fn decimal_sorts_by_value_not_by_text() {
	// "10" sorts before "9" as text, so a text-ordered decimal key mis-orders every scan.
	assert!(enc_dec("10") < enc_dec("9"));
	assert!(enc_dec("2.5") < enc_dec("2.05"));
	assert!(enc_dec("0.1") < enc_dec("0"));
	assert!(enc_dec("0") < enc_dec("-0.1"));
	assert!(enc_dec("-1") < enc_dec("-10"));
	assert!(enc_dec("100") < enc_dec("99.9"), "must hold across a digit-count boundary");
}

#[test]
fn int_uint_decimal_round_trip_the_awkward_cases() {
	// ordering rewrites are worthless if a negative or a trailing-zero scale cannot be read back.
	for v in [0i64, 1, -1, 255, -256, i64::MIN, i64::MAX] {
		let b = enc_int(v);
		let mut d = KeyDeserializer::from_bytes(&b);
		assert_eq!(d.read_int().unwrap(), Int(BigInt::from(v)), "int {v}");
	}
	for v in [0u64, 1, 255, 256, u64::MAX] {
		let b = enc_uint(v);
		let mut d = KeyDeserializer::from_bytes(&b);
		assert_eq!(d.read_uint().unwrap(), Uint(BigInt::from(v)), "uint {v}");
	}
	for s in ["0", "0.00", "1.0", "1.00", "-3.14159", "1e10", "-0.0001"] {
		let b = enc_dec(s);
		let mut d = KeyDeserializer::from_bytes(&b);
		let want = Decimal::from_str(s).unwrap();
		assert_eq!(d.read_decimal().unwrap().to_string(), want.to_string(), "decimal {s}");
	}
}

fn versioned_uuid(n: u128, version: u128) -> Uuid {
	Uuid::from_u128((n & !(0xf << 76)) | (version << 76))
}

fn ascending_samples() -> Vec<(&'static str, Vec<Value>)> {
	vec![
		("Boolean", vec![Value::Boolean(false), Value::Boolean(true)]),
		(
			"Float4",
			[-1e30f32, -1.5, -0.0, 1.5, 1e30]
				.into_iter()
				.map(|f| Value::Float4(OrderedF32::try_from(f).unwrap()))
				.collect(),
		),
		(
			"Float8",
			[-1e300f64, -1.5, -0.0, 1.5, 1e300]
				.into_iter()
				.map(|f| Value::Float8(OrderedF64::try_from(f).unwrap()))
				.collect(),
		),
		("Int1", [i8::MIN, -1, 0, 1, i8::MAX].into_iter().map(Value::Int1).collect()),
		("Int2", [i16::MIN, -1, 0, 1, i16::MAX].into_iter().map(Value::Int2).collect()),
		("Int4", [i32::MIN, -1, 0, 1, i32::MAX].into_iter().map(Value::Int4).collect()),
		("Int8", [i64::MIN, -1, 0, 1, i64::MAX].into_iter().map(Value::Int8).collect()),
		("Int16", [i128::MIN, -1, 0, 1, i128::MAX].into_iter().map(Value::Int16).collect()),
		("Uint1", [0u8, 1, u8::MAX].into_iter().map(Value::Uint1).collect()),
		("Uint2", [0u16, 1, 255, 256, u16::MAX].into_iter().map(Value::Uint2).collect()),
		("Uint4", [0u32, 1, 255, 256, u32::MAX].into_iter().map(Value::Uint4).collect()),
		("Uint8", [0u64, 1, 255, 256, u64::MAX].into_iter().map(Value::Uint8).collect()),
		("Uint16", [0u128, 1, 255, 256, u128::MAX].into_iter().map(Value::Uint16).collect()),
		(
			"Utf8",
			["", "a", "ab", "abc", "b", "z", "\u{1f600}"]
				.into_iter()
				.map(|s| Value::Utf8(s.to_string()))
				.collect(),
		),
		(
			"Blob",
			[vec![], vec![0x00], vec![0x00, 0x00], vec![0x01], vec![0x01, 0x00], vec![0xff]]
				.into_iter()
				.map(|b| Value::Blob(Blob::from(b)))
				.collect(),
		),
		(
			"Int",
			[-300i64, -256, -255, -1, 0, 1, 255, 256, 300]
				.into_iter()
				.map(|v| Value::Int(Int(BigInt::from(v))))
				.collect(),
		),
		(
			"Uint",
			[0u128, 1, 255, 256, u64::MAX as u128, u128::MAX]
				.into_iter()
				.map(|v| Value::Uint(Uint(BigInt::from(v))))
				.collect(),
		),
		(
			"Decimal",
			["-100", "-10", "-1", "-0.1", "-0.01", "0", "0.01", "0.1", "1", "9", "10", "100"]
				.into_iter()
				.map(|s| Value::Decimal(Decimal::from_str(s).unwrap()))
				.collect(),
		),
		(
			"Date",
			[(1900, 1, 1), (2024, 6, 14), (2024, 6, 15), (2024, 7, 1), (2100, 12, 31)]
				.into_iter()
				.map(|(y, m, d)| Value::Date(Date::from_ymd(y, m, d).unwrap()))
				.collect(),
		),
		(
			"DateTime",
			[
				(2024, 6, 15, 0, 0, 0),
				(2024, 6, 15, 12, 30, 44),
				(2024, 6, 15, 12, 30, 45),
				(2025, 1, 1, 0, 0, 0),
			]
			.into_iter()
			.map(|(y, mo, d, h, mi, s)| {
				Value::DateTime(DateTime::from_ymd_hms(y, mo, d, h, mi, s).unwrap())
			})
			.collect(),
		),
		(
			"Time",
			[(0, 0, 0), (12, 30, 44), (12, 30, 45), (23, 59, 59)]
				.into_iter()
				.map(|(h, m, s)| Value::Time(Time::from_hms(h, m, s).unwrap()))
				.collect(),
		),
		(
			"Duration",
			[-3600i64, -1, 0, 1, 3600]
				.into_iter()
				.map(|s| Value::Duration(Duration::from_seconds(s).unwrap()))
				.collect(),
		),
		(
			"Uuid4",
			[0u128, 1, 1 << 64, u128::MAX]
				.into_iter()
				.map(|n| Value::Uuid4(Uuid4(versioned_uuid(n, 4))))
				.collect(),
		),
		(
			"Uuid7",
			[0u128, 1, 1 << 64, u128::MAX]
				.into_iter()
				.map(|n| Value::Uuid7(Uuid7(versioned_uuid(n, 7))))
				.collect(),
		),
		(
			"IdentityId",
			[0u128, 1, 1 << 64, u128::MAX]
				.into_iter()
				.map(|n| Value::IdentityId(IdentityId(Uuid7(versioned_uuid(n, 7)))))
				.collect(),
		),
	]
}

fn enc_value(value: &Value) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_value(value);
	s.to_encoded_key().to_vec()
}

fn enc_value_dir(value: &Value, direction: SortOrder) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_value_with_direction(value, direction);
	s.to_encoded_key().to_vec()
}

#[test]
fn every_value_type_encodes_descending() {
	// descending is the house default; a type encoded ascending silently reverses its scans.
	for (label, values) in ascending_samples() {
		for pair in values.windows(2) {
			assert!(
				enc_value(&pair[1]) < enc_value(&pair[0]),
				"{label}: {:?} is greater than {:?} so it must encode smaller",
				pair[1],
				pair[0]
			);
		}
	}
}

#[test]
fn asc_direction_reverses_every_value_type() {
	// a type missing from keycode_type_descending stays descending under an explicit ASC.
	for (label, values) in ascending_samples() {
		for pair in values.windows(2) {
			assert!(
				enc_value_dir(&pair[0], SortOrder::Asc) < enc_value_dir(&pair[1], SortOrder::Asc),
				"{label}: ASC must invert the default order for {:?} vs {:?}",
				pair[0],
				pair[1]
			);
		}
	}
}

#[test]
fn every_value_type_round_trips() {
	// an ordering rewrite that cannot be read back loses data rather than mis-sorting it.
	for (label, values) in ascending_samples() {
		for value in &values {
			let bytes = enc_value(value);
			let mut d = KeyDeserializer::from_bytes(&bytes);
			assert_eq!(&d.read_value().unwrap(), value, "{label}");
		}
	}
}

#[test]
fn the_leading_column_of_a_composite_key_dominates() {
	// a trailing column that outranks the leading one would break every multi-column scan.
	let mut low_high = KeySerializer::new();
	low_high.extend_value(&Value::Int4(1)).extend_value(&Value::Int4(9));
	let mut high_low = KeySerializer::new();
	high_low.extend_value(&Value::Int4(2)).extend_value(&Value::Int4(0));
	assert!(high_low.to_encoded_key().to_vec() < low_high.to_encoded_key().to_vec());
}
