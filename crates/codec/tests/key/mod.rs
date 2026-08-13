// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod container_keys;
mod deserializer;
mod encoded;
mod serializer;

use std::{borrow::Cow, f32, f64};

use reifydb_codec::key::{serializer::KeySerializer, *};
use reifydb_value::{
	util::hex::encode,
	value::{Value, ordered_f32::OrderedF32, ordered_f64::OrderedF64},
};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

const PI_F32: f32 = f32::consts::PI;
const PI_F64: f64 = f64::consts::PI;

#[test]
fn test_u128_varint_roundtrip_and_descending_order() {
	let values: Vec<u128> = vec![
		0,
		1,
		2,
		126,
		127,
		128,
		129,
		(1 << 14) - 1,
		1 << 14,
		(1 << 21) - 1,
		1 << 21,
		(1 << 28) - 1,
		1 << 28,
		(1 << 35) - 1,
		1 << 35,
		(1 << 42) - 1,
		1 << 42,
		(1 << 49) - 1,
		1 << 49,
		(1 << 56) - 1,
		1 << 56,
		(1u128 << 63) - 1,
		1u128 << 63,
		u64::MAX as u128 - 1,
		u64::MAX as u128,
		u64::MAX as u128 + 1,
		1u128 << 100,
		u128::MAX - 1,
		u128::MAX,
	];

	for &v in &values {
		let mut buf = Vec::new();
		encode_u128_varint(v, &mut buf);
		let mut slice = buf.as_slice();
		let decoded = decode_u128_varint(&mut slice).unwrap();
		assert_eq!(decoded, v, "roundtrip failed for {}", v);
		assert!(slice.is_empty(), "trailing bytes after decoding {}", v);
	}

	// keycode is descending: a strictly larger value must encode to a lexicographically smaller key.
	let mut sorted = values.clone();
	sorted.sort();
	sorted.dedup();
	let mut prev: Option<(u128, Vec<u8>)> = None;
	for &v in &sorted {
		let mut buf = Vec::new();
		encode_u128_varint(v, &mut buf);
		if let Some((pv, pe)) = &prev {
			assert!(*pv < v);
			assert!(buf < *pe, "not descending: {} -> {:?} should sort before {} -> {:?}", v, buf, pv, pe);
		}
		prev = Some((v, buf));
	}
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
enum Key<'a> {
	Unit,
	NewType(String),
	Tuple(bool, #[serde(with = "serde_bytes")] Vec<u8>, u64),
	Cow(
		#[serde(with = "serde_bytes")]
		#[serde(borrow)]
		Cow<'a, [u8]>,
		bool,
		#[serde(borrow)] Cow<'a, str>,
	),
}

macro_rules! test_serde {
        ( $( $name:ident: $input:expr => $expect:literal, )* ) => {
        $(
            #[test]
            fn $name(){
                let mut input = $input;
                let expect = $expect;
                let output = serialize(&input);
                assert_eq!(encode(&output), expect, "encode failed");

                let expect = input;
                input = deserialize(&output).unwrap();
                assert_eq!(input, expect, "decode failed");
            }
        )*
        };
    }

test_serde! {
bool_false: false => "01",
bool_true: true => "00",

f32_min: f32::MIN => "ff7fffff",
f32_neg_inf: f32::NEG_INFINITY => "ff800000",
f32_neg_pi: -PI_F32 => "c0490fdb",
f32_neg_zero: -0f32 => "80000000",
f32_zero: 0f32 => "7fffffff",
f32_pi: PI_F32 => "3fb6f024",
f32_max: f32::MAX => "00800000",
f32_inf: f32::INFINITY => "007fffff",

f64_min: f64::MIN => "ffefffffffffffff",
f64_neg_inf: f64::NEG_INFINITY => "fff0000000000000",
f64_neg_pi: -PI_F64 => "c00921fb54442d18",
f64_neg_zero: -0f64 => "8000000000000000",
f64_zero: 0f64 => "7fffffffffffffff",
f64_pi: PI_F64 => "3ff6de04abbbd2e7",
f64_max: f64::MAX => "0010000000000000",
f64_inf: f64::INFINITY => "000fffffffffffff",

i8_min: i8::MIN => "ff",
i8_neg_1: -1i8 => "80",
i8_0: 0i8 => "7f",
i8_1: 1i8 => "7e",
i8_max: i8::MAX => "00",

i16_min: i16::MIN => "ffff",
i16_neg_1: -1i16 => "8000",
i16_0: 0i16 => "7fff",
i16_1: 1i16 => "7ffe",
i16_max: i16::MAX => "0000",

i32_min: i32::MIN => "ffffffff",
i32_neg_1: -1i32 => "80000000",
i32_0: 0i32 => "7fffffff",
i32_1: 1i32 => "7ffffffe",
i32_max: i32::MAX => "00000000",

i64_min: i64::MIN => "ffffffffffffffff",
i64_neg_65535: -65535i64 => "800000000000fffe",
i64_neg_1: -1i64 => "8000000000000000",
i64_0: 0i64 => "7fffffffffffffff",
i64_1: 1i64 => "7ffffffffffffffe",
i64_65535: 65535i64 => "7fffffffffff0000",
i64_max: i64::MAX => "0000000000000000",

i128_min: i128::MIN => "ffffffffffffffffffffffffffffffff",
i128_neg_1: -1i128 => "80000000000000000000000000000000",
i128_0: 0i128 => "7fffffffffffffffffffffffffffffff",
i128_1: 1i128 => "7ffffffffffffffffffffffffffffffe",
i128_max: i128::MAX => "00000000000000000000000000000000",

u8_min: u8::MIN => "ff",
u8_1: 1_u8 => "fe",
u8_255: 255_u8 => "00",

u16_min: u16::MIN => "ffff",
u16_1: 1_u16 => "fffe",
u16_255: 255_u16 => "ff00",
u16_65535: u16::MAX => "0000",

u32_min: u32::MIN => "ffffffff",
u32_1: 1_u32 => "fffffffe",
u32_65535: 65535_u32 => "ffff0000",
u32_max: u32::MAX => "00000000",

u64_min: u64::MIN => "ffffffffffffffff",
u64_1: 1_u64 => "fffffffffffffffe",
u64_65535: 65535_u64 => "ffffffffffff0000",
u64_max: u64::MAX => "0000000000000000",

u128_min: u128::MIN => "ffffffffffffffffffffffffffffffff",
u128_1: 1_u128 => "fffffffffffffffffffffffffffffffe",
u128_65535: 65535_u128 => "ffffffffffffffffffffffffffff0000",
u128_max: u128::MAX => "00000000000000000000000000000000",

bytes: ByteBuf::from(vec![0x01, 0xff]) => "01ff00ffff",
bytes_empty: ByteBuf::new() => "ffff",
bytes_escape: ByteBuf::from(vec![0x00, 0x01, 0x02]) => "000102ffff",

string: "foo".to_string() => "666f6fffff",
string_empty: "".to_string() => "ffff",
string_escape: "foo\x00bar".to_string() => "666f6f00626172ffff",
string_utf8: "👋".to_string() => "f09f918bffff",

tuple: (true, u64::MAX, ByteBuf::from(vec![0x00, 0x01])) => "0000000000000000000001ffff",
array_bool: [false, true, false] => "010001",
vec_bool: vec![false, true, false] => "010001",
vec_u64: vec![u64::MIN, u64::MAX, 65535_u64] => "ffffffffffffffff0000000000000000ffffffffffff0000",

enum_unit: Key::Unit => "00",
enum_newtype: Key::NewType("foo".to_string()) => "01666f6fffff",
enum_tuple: Key::Tuple(false, vec![0x00, 0x01], u64::MAX) => "02010001ffff0000000000000000",
enum_cow: Key::Cow(vec![0x00, 0x01].into(), false, String::from("foo").into()) => "030001ffff01666f6fffff",
enum_cow_borrow: Key::Cow([0x00, 0x01].as_slice().into(), false, "foo".into()) => "030001ffff01666f6fffff",

value_none: Value::none() => "001a",
value_bool: Value::Boolean(true) => "0100",
value_float4: Value::Float4(OrderedF32::try_from(PI_F32).unwrap()) => "023fb6f024",
value_float8: Value::Float8(OrderedF64::try_from(PI_F64).unwrap()) => "033ff6de04abbbd2e7",
value_int1: Value::Int1(-1) => "0480",
value_int4: Value::Int4(123456) => "067ffe1dbf",
value_int8: Value::Int8(31415926) => "077ffffffffe20a189",
value_int16: Value::Int16(-123456789012345678901234567890i128) => "08800000018ee90ff6c373e0ee4e3f0ad1",
value_string: Value::Utf8("foo".to_string()) => "09666f6fffff",
value_uint1: Value::Uint1(255) => "0a00",
value_uint2: Value::Uint2(65535) => "0b0000",
value_uint4: Value::Uint4(4294967295) => "0c00000000",
value_uint8: Value::Uint8(18446744073709551615) => "0d0000000000000000",
value_uint16: Value::Uint16(340282366920938463463374607431768211455u128) => "0e00000000000000000000000000000000",

option_none_bool: None::<bool> => "00",
option_some_true: Some(true) => "0100",
option_some_false: Some(false) => "0101",

option_none_f32: None::<f32> => "00",
option_some_f32: Some(PI_F32) => "013fb6f024",

option_none_f64: None::<f64> => "00",
option_some_f64: Some(PI_F64) => "013ff6de04abbbd2e7",

option_none_i8: None::<i8> => "00",
option_some_i8: Some(0i8) => "017f",

option_none_i16: None::<i16> => "00",
option_some_i16: Some(0i16) => "017fff",

option_none_i32: None::<i32> => "00",
option_some_i32: Some(0i32) => "017fffffff",

option_none_i64: None::<i64> => "00",
option_some_i64: Some(0i64) => "017fffffffffffffff",

option_none_i128: None::<i128> => "00",
option_some_i128: Some(0i128) => "017fffffffffffffffffffffffffffffff",

option_none_u8: None::<u8> => "00",
option_some_u8: Some(0u8) => "01ff",

option_none_u16: None::<u16> => "00",
option_some_u16: Some(0u16) => "01ffff",

option_none_u32: None::<u32> => "00",
option_some_u32: Some(0u32) => "01ffffffff",

option_none_u64: None::<u64> => "00",
option_some_u64: Some(0u64) => "01ffffffffffffffff",

option_none_u128: None::<u128> => "00",
option_some_u128: Some(0u128) => "01ffffffffffffffffffffffffffffffff",

option_none_string: None::<String> => "00",
option_some_string: Some("foo".to_string()) => "01666f6fffff",
option_some_empty_string: Some("".to_string()) => "01ffff",

option_none_bytes: None::<ByteBuf> => "00",
option_some_bytes: Some(ByteBuf::from(vec![0x01, 0xff])) => "0101ff00ffff",

option_nested_none: None::<Option<bool>> => "00",
option_nested_some_none: Some(None::<bool>) => "0100",
option_nested_some_some_true: Some(Some(true)) => "010100",
option_nested_some_some_false: Some(Some(false)) => "010101",

option_nested_none_i32: None::<Option<i32>> => "00",
option_nested_some_none_i32: Some(None::<i32>) => "0100",
option_nested_some_some_i32: Some(Some(0i32)) => "01017fffffff",

option_nested_some_some_string: Some(Some("foo".to_string())) => "0101666f6fffff",

option_triple_none: None::<Option<Option<bool>>> => "00",
option_triple_some_none: Some(None::<Option<bool>>) => "0100",
option_triple_some_some_none: Some(Some(None::<bool>)) => "010100",
option_triple_some_some_some: Some(Some(Some(true))) => "01010100",}

#[test]
fn test_option_ordering() {
	// Descending order (None > Some(MAX) > Some(0) > Some(MIN)) must byte-sort ascending.
	let none = serialize(&None::<i32>);
	let some_max = serialize(&Some(i32::MAX));
	let some_zero = serialize(&Some(0i32));
	let some_min = serialize(&Some(i32::MIN));
	assert!(none < some_max);
	assert!(some_max < some_zero);
	assert!(some_zero < some_min);
}

#[test]
fn test_nested_option_ordering() {
	let none = serialize(&None::<Option<bool>>);
	let some_none = serialize(&Some(None::<bool>));
	let some_some_true = serialize(&Some(Some(true)));
	let some_some_false = serialize(&Some(Some(false)));
	assert!(none < some_none);
	assert!(some_none < some_some_true);
	assert!(some_some_true < some_some_false);
}

#[test]
fn test_key_serializer() {
	let mut s = KeySerializer::new();
	s.extend_bool(true);
	assert_eq!(s.finish(), vec![0x00]);

	let mut s = KeySerializer::new();
	s.extend_bool(false);
	assert_eq!(s.finish(), vec![0x01]);

	let mut s = KeySerializer::new();
	s.extend_u64(0u64);
	assert_eq!(s.finish(), vec![0xff; 8]);

	let mut s = KeySerializer::new();
	s.extend_i64(0i64);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_f32(0.0f32);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_f64(0.0f64);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_bytes(b"foo");
	assert_eq!(s.finish(), vec![0x66, 0x6f, 0x6f, 0xff, 0xff]);

	let mut s = KeySerializer::with_capacity(32);
	s.extend_bool(true).extend_u32(1u32).extend_i16(-1i16).extend_bytes(b"test");
	let result = s.finish();
	assert!(!result.is_empty());
	assert!(result.len() >= 10);
}

#[test]
fn the_two_u64_key_encodings_sort_in_opposite_directions() {
	// encode_u64 is inverted for the descending row keycode; encode_u64_asc stays plain for forward scans.
	let ordered = [0u64, 1, 255, 256, 65_535, 65_536, u64::MAX - 1, u64::MAX];

	for pair in ordered.windows(2) {
		let (lo, hi) = (pair[0], pair[1]);
		assert!(encode_u64_asc(lo) < encode_u64_asc(hi), "ascending encoding must keep {lo} below {hi}");
		assert!(encode_u64(lo) > encode_u64(hi), "inverted encoding must put {lo} above {hi}");
	}
}

#[test]
fn every_fixed_width_key_encoding_round_trips() {
	// a decoder that misses or doubles the inversion must not silently read a wrong value instead of failing.
	for value in [0u64, 1, 255, 256, 1_700_000_000_000, u64::MAX - 1, u64::MAX] {
		assert_eq!(decode_u64(encode_u64(value)), value, "inverted u64 round trip");
		assert_eq!(decode_u64_asc(encode_u64_asc(value)), value, "ascending u64 round trip");
	}
	for value in [0u128, 1, u128::from(u64::MAX), u128::MAX] {
		assert_eq!(decode_u128_asc(encode_u128_asc(value)), value, "ascending u128 round trip");
	}
}
