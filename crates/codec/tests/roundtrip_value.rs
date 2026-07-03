// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Round-trip coverage for the canonical value codec: every Value variant, the none-with-inner-type
//! matrix across option depths (the bug class that motivated the codec crate), and adversarial
//! truncation (every prefix of a valid encoding must decode to Err, never panic).

use std::str::FromStr;

use num_bigint::BigInt;
use reifydb_codec::value::{decode_value, encode_value};
use reifydb_value::value::{
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
	value_type::ValueType,
};

fn sample_values() -> Vec<Value> {
	vec![
		Value::none(),
		Value::none_of(ValueType::Duration),
		Value::none_of(ValueType::Option(Box::new(ValueType::Duration))),
		Value::none_of(ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Duration))))),
		Value::none_of(ValueType::List(Box::new(ValueType::Int4))),
		Value::none_of(ValueType::Record(vec![
			("a".to_string(), ValueType::Int4),
			("b".to_string(), ValueType::Option(Box::new(ValueType::Utf8))),
		])),
		Value::none_of(ValueType::Tuple(vec![ValueType::Boolean, ValueType::Blob])),
		Value::Boolean(true),
		Value::Boolean(false),
		Value::Float4(OrderedF32::try_from(3.5f32).unwrap()),
		Value::Float8(OrderedF64::try_from(-2.25f64).unwrap()),
		Value::Int1(i8::MIN),
		Value::Int2(i16::MAX),
		Value::Int4(-42),
		Value::Int8(i64::MIN),
		Value::Int16(i128::MAX),
		Value::Utf8(String::new()),
		Value::Utf8("hello \u{1F980} world".to_string()),
		Value::Uint1(u8::MAX),
		Value::Uint2(0),
		Value::Uint4(u32::MAX),
		Value::Uint8(u64::MAX),
		Value::Uint16(u128::MAX),
		Value::Date(Date::from_days_since_epoch(0).unwrap()),
		Value::Date(Date::from_days_since_epoch(-719162).unwrap()),
		Value::DateTime(DateTime::from_nanos(1_700_000_000_000_000_000)),
		Value::Time(Time::from_nanos_since_midnight(0).unwrap()),
		Value::Duration(Duration::new(1, 2, 3_000_000_000).unwrap()),
		Value::IdentityId(IdentityId::new(Uuid7(uuid::Uuid::from_u128(7)))),
		Value::Uuid4(Uuid4(uuid::Uuid::nil())),
		Value::Uuid7(Uuid7(uuid::Uuid::from_u128(0x0123_4567_89ab_cdef))),
		Value::Blob(Blob::new(vec![])),
		Value::Blob(Blob::new(vec![0x00, 0xff, 0x7f])),
		Value::Int(Int(BigInt::parse_bytes(b"-123456789012345678901234567890", 10).unwrap())),
		Value::Uint(Uint(BigInt::parse_bytes(b"987654321098765432109876543210", 10).unwrap())),
		Value::Decimal(Decimal::from_str("-3.14159265358979").unwrap()),
		Value::Any(Box::new(Value::Int4(5))),
		Value::Any(Box::new(Value::Any(Box::new(Value::Utf8("nested".to_string()))))),
		Value::Any(Box::new(Value::none_of(ValueType::Duration))),
		Value::DictionaryId(DictionaryEntryId::U1(1)),
		Value::DictionaryId(DictionaryEntryId::U2(300)),
		Value::DictionaryId(DictionaryEntryId::U4(70_000)),
		Value::DictionaryId(DictionaryEntryId::U8(5_000_000_000)),
		Value::DictionaryId(DictionaryEntryId::U16(u128::MAX)),
		Value::Type(ValueType::Duration),
		Value::Type(ValueType::Option(Box::new(ValueType::Int4))),
		Value::Type(ValueType::Record(vec![("x".to_string(), ValueType::Float8)])),
		Value::List(vec![]),
		Value::List(vec![Value::Int4(1), Value::Utf8("two".to_string()), Value::none_of(ValueType::Int4)]),
		Value::Record(vec![
			("name".to_string(), Value::Utf8("reify".to_string())),
			("count".to_string(), Value::Int8(9)),
		]),
		Value::Tuple(vec![Value::Boolean(true), Value::Blob(Blob::new(vec![1, 2, 3]))]),
	]
}

fn assert_value_eq(expected: &Value, actual: &Value) {
	match (expected, actual) {
		(
			Value::None {
				inner: l,
			},
			Value::None {
				inner: r,
			},
		) => {
			assert_eq!(l, r, "none inner type must round-trip exactly");
		}
		_ => assert_eq!(expected, actual),
	}
}

#[test]
fn every_sample_value_round_trips() {
	for value in sample_values() {
		let encoded = encode_value(&value).unwrap_or_else(|e| panic!("encode failed for {value:?}: {e}"));
		let decoded = decode_value(&encoded).unwrap_or_else(|e| panic!("decode failed for {value:?}: {e}"));
		assert_value_eq(&value, &decoded);
	}
}

#[test]
fn none_inner_matrix_round_trips() {
	let scalar_inners = [
		ValueType::Boolean,
		ValueType::Float4,
		ValueType::Float8,
		ValueType::Int1,
		ValueType::Int2,
		ValueType::Int4,
		ValueType::Int8,
		ValueType::Int16,
		ValueType::Utf8,
		ValueType::Uint1,
		ValueType::Uint2,
		ValueType::Uint4,
		ValueType::Uint8,
		ValueType::Uint16,
		ValueType::Date,
		ValueType::DateTime,
		ValueType::Time,
		ValueType::Duration,
		ValueType::IdentityId,
		ValueType::Uuid4,
		ValueType::Uuid7,
		ValueType::Blob,
		ValueType::Int,
		ValueType::Uint,
		ValueType::Decimal,
		ValueType::Any,
		ValueType::DictionaryId,
	];
	for base in scalar_inners {
		for depth in 0..=3u8 {
			let inner = (0..depth).fold(base.clone(), |ty, _| ValueType::Option(Box::new(ty)));
			let value = Value::none_of(inner.clone());
			let encoded = encode_value(&value)
				.unwrap_or_else(|e| panic!("encode failed for none of {inner:?}: {e}"));
			match decode_value(&encoded) {
				Ok(Value::None {
					inner: decoded_inner,
				}) => {
					assert_eq!(inner, decoded_inner, "inner type mismatch at depth {depth}");
				}
				other => panic!("expected none of {inner:?}, got {other:?}"),
			}
		}
	}
}

#[test]
fn none_inner_deeper_than_tag_cap_round_trips_via_extended_typeinfo() {
	let base = ValueType::Duration;
	for depth in 4..=6u32 {
		let inner = (0..depth).fold(base.clone(), |ty, _| ValueType::Option(Box::new(ty)));
		let value = Value::none_of(inner.clone());
		let encoded = encode_value(&value).unwrap_or_else(|e| panic!("encode failed at depth {depth}: {e}"));
		match decode_value(&encoded) {
			Ok(Value::None {
				inner: decoded_inner,
			}) => assert_eq!(inner, decoded_inner, "inner type mismatch at depth {depth}"),
			other => panic!("expected none at depth {depth}, got {other:?}"),
		}
	}
}

#[test]
fn truncated_encodings_error_and_never_panic() {
	for value in sample_values() {
		let encoded = encode_value(&value).unwrap();
		for cut in 0..encoded.len() {
			match decode_value(&encoded[..cut]) {
				Ok(decoded) => panic!(
					"truncated encoding of {value:?} at {cut}/{} unexpectedly decoded to {decoded:?}",
					encoded.len()
				),
				Err(_) => {}
			}
		}
	}
}

#[test]
fn unknown_and_reserved_tag_bytes_error() {
	assert!(decode_value(&[0x3F]).is_err(), "kind 63 is reserved");
	assert!(decode_value(&[0x20]).is_err(), "kind 32 is unassigned");
	assert!(decode_value(&[0xFF]).is_err(), "0xFF alone is not a valid value encoding");
}

#[test]
fn trailing_bytes_are_rejected() {
	let mut encoded = encode_value(&Value::Boolean(true)).unwrap();
	encoded.push(0x00);
	assert!(decode_value(&encoded).is_err());
}
