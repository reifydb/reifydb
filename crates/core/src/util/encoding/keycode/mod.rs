// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

//! Order-preserving codec used to turn typed keys into the bytes that go on disk.
//!
//! Encoded byte sequences sort lexicographically in the same order as the logical keys they represent, so range scans
//! over the storage tier produce results in natural key order without any decode pass. Submodules cover the
//! catalog-specific key encodings, the generic `Serializer` and `Deserializer` pair, and per-type encoders for
//! booleans, floats, and signed and unsigned integers.
//!
//! Invariant: the codec is order-preserving. For any two values `a < b` in their natural ordering, their encoded bytes
//! must satisfy `encode(a) < encode(b)` lexicographically. Storage range queries, CDC, and replication all rely on this
//! property; breaking it silently corrupts every range-scan-based operation in the workspace.

use serde::{Deserialize, Serialize};

pub mod catalog;
pub mod deserialize;
pub mod deserializer;
pub mod serialize;
pub mod serializer;
pub mod varint;

use std::{f32, f64};

use reifydb_type::{
	Result,
	error::{Error, TypeError},
};

use crate::{
	encoded::key::EncodedKey,
	util::encoding::keycode::{deserialize::Deserializer, serialize::Serializer},
};

pub trait ByteSink {
	fn push(&mut self, byte: u8);
	fn extend_from_slice(&mut self, slice: &[u8]);
}

impl ByteSink for Vec<u8> {
	fn push(&mut self, byte: u8) {
		Vec::push(self, byte);
	}
	fn extend_from_slice(&mut self, slice: &[u8]) {
		Vec::extend_from_slice(self, slice);
	}
}

impl ByteSink for EncodedKey {
	fn push(&mut self, byte: u8) {
		EncodedKey::push(self, byte);
	}
	fn extend_from_slice(&mut self, slice: &[u8]) {
		EncodedKey::extend_from_slice(self, slice);
	}
}

pub fn encode_bool(value: bool) -> u8 {
	if value {
		0x00
	} else {
		0x01
	}
}

pub fn encode_f32(value: f32) -> [u8; 4] {
	let bits = value.to_bits();
	if value.is_sign_negative() {
		bits.to_be_bytes()
	} else {
		(!(bits ^ 0x80000000)).to_be_bytes()
	}
}

pub fn encode_f64(value: f64) -> [u8; 8] {
	let bits = value.to_bits();
	if value.is_sign_negative() {
		bits.to_be_bytes()
	} else {
		(!(bits ^ 0x8000000000000000)).to_be_bytes()
	}
}

pub fn encode_i8(value: i8) -> [u8; 1] {
	(!(value as u8 ^ 0x80)).to_be_bytes()
}

pub fn encode_i16(value: i16) -> [u8; 2] {
	(!(value as u16 ^ 0x8000)).to_be_bytes()
}

pub fn encode_i32(value: i32) -> [u8; 4] {
	(!(value as u32 ^ 0x80000000)).to_be_bytes()
}

pub fn encode_i64(value: i64) -> [u8; 8] {
	(!(value as u64 ^ 0x8000000000000000)).to_be_bytes()
}

pub fn encode_i64_varint<B: ByteSink>(value: i64, output: &mut B) {
	let mut buf = Vec::with_capacity(9);
	varint::encode_i64_varint(value, &mut buf);
	for b in buf.iter_mut() {
		*b = !*b;
	}
	output.extend_from_slice(&buf);
}

pub fn encode_i128(value: i128) -> [u8; 16] {
	(!(value as u128 ^ 0x80000000000000000000000000000000)).to_be_bytes()
}

pub fn encode_u8(value: u8) -> u8 {
	!value
}

pub fn encode_u16(value: u16) -> [u8; 2] {
	(!value).to_be_bytes()
}

pub fn encode_u32(value: u32) -> [u8; 4] {
	(!value).to_be_bytes()
}

pub fn encode_u32_varint<B: ByteSink>(value: u32, output: &mut B) {
	encode_u64_varint(value as u64, output);
}

pub fn encode_u64(value: u64) -> [u8; 8] {
	(!value).to_be_bytes()
}

pub fn encode_u64_varint<B: ByteSink>(value: u64, output: &mut B) {
	let mut buf = Vec::with_capacity(9);
	varint::encode_u64_varint(value, &mut buf);
	for b in buf.iter_mut() {
		*b = !*b;
	}
	output.extend_from_slice(&buf);
}

pub fn decode_i64_varint(input: &mut &[u8]) -> Result<i64> {
	if input.is_empty() {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding i64 varint".to_string(),
		}));
	}
	let first = !input[0];
	let len = if first >= 0x80 {
		if first < 0xc0 {
			1
		} else if first < 0xfe {
			2
		} else {
			9
		}
	} else if first >= 0x40 {
		1
	} else if first >= 0x20 {
		2
	} else {
		9
	};

	if input.len() < len {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding i64 varint".to_string(),
		}));
	}

	let mut buf = input[..len].to_vec();
	for b in buf.iter_mut() {
		*b = !*b;
	}
	let mut slice = &buf[..];
	let v = varint::decode_i64_varint(&mut slice).ok_or_else(|| {
		Error::from(TypeError::SerdeKeycode {
			message: "failed to decode signed varint".to_string(),
		})
	})?;
	*input = &input[len..];
	Ok(v)
}

pub fn decode_u64_varint(input: &mut &[u8]) -> Result<u64> {
	if input.is_empty() {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding varint".to_string(),
		}));
	}
	let first = !input[0];
	let prefix = first.leading_ones() as usize;
	let len = if prefix == 0 {
		1
	} else if prefix < 8 {
		prefix + 1
	} else {
		9
	};

	if input.len() < len {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding varint".to_string(),
		}));
	}

	let mut buf = input[..len].to_vec();
	for b in buf.iter_mut() {
		*b = !*b;
	}
	let mut slice = &buf[..];
	let v = varint::decode_u64_varint(&mut slice).unwrap();
	*input = &input[len..];
	Ok(v)
}

pub fn encode_u128(value: u128) -> [u8; 16] {
	(!value).to_be_bytes()
}

pub fn encode_bytes<B: ByteSink>(bytes: &[u8], output: &mut B) {
	let mut start = 0;
	while let Some(pos) = bytes[start..].iter().position(|&b| b == 0xff) {
		let end = start + pos;
		output.extend_from_slice(&bytes[start..end]);
		output.extend_from_slice(&[0xff, 0x00]);
		start = end + 1;
	}
	output.extend_from_slice(&bytes[start..]);
	output.extend_from_slice(&[0xff, 0xff]);
}

#[macro_export]
macro_rules! key_prefix {
    ($($arg:tt)*) => {
        &EncodedKey::new((&format!($($arg)*)).as_bytes().to_vec())
    };
}

pub fn serialize<T: Serialize>(key: &T) -> Vec<u8> {
	let mut serializer = Serializer {
		output: Vec::new(),
	};

	key.serialize(&mut serializer).expect("key must be serializable");
	serializer.output
}

pub fn deserialize<'a, T: Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
	let mut deserializer = Deserializer::from_bytes(input);
	let t = T::deserialize(&mut deserializer)?;
	if !deserializer.input.is_empty() {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: format!(
				"unexpected trailing bytes {:x?} at end of key {input:x?}",
				deserializer.input,
			),
		}));
	}
	Ok(t)
}

#[cfg(test)]
pub mod tests {
	use std::borrow::Cow;

	const PI_F32: f32 = f32::consts::PI;
	const PI_F64: f64 = f64::consts::PI;

	use reifydb_type::{
		util::hex::encode,
		value::{Value, ordered_f32::OrderedF32, ordered_f64::OrderedF64},
	};
	use serde_bytes::ByteBuf;

	use super::*;
	use crate::util::encoding::keycode::serializer::KeySerializer;

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

	i64_min: i64::MIN => "fe7fffffffffffffff",
	i64_neg_65535: -65535i64 => "fe000000000000fffe",
	i64_neg_1: -1i64 => "80",
	i64_0: 0i64 => "7f",
	i64_1: 1i64 => "7e",
	i64_65535: 65535i64 => "01ffffffffffff0000",
	i64_max: i64::MAX => "018000000000000000",

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

	u32_min: u32::MIN => "ff",
	u32_1: 1_u32 => "fe",
	u32_65535: 65535_u32 => "3f0000",
	u32_max: u32::MAX => "0f00000000",

	u64_min: u64::MIN => "ff",
	u64_1: 1_u64 => "fe",
	u64_65535: 65535_u64 => "3f0000",
	u64_max: u64::MAX => "000000000000000000",

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

	tuple: (true, u64::MAX, ByteBuf::from(vec![0x00, 0x01])) => "000000000000000000000001ffff",
	array_bool: [false, true, false] => "010001",
	vec_bool: vec![false, true, false] => "010001",
	vec_u64: vec![u64::MIN, u64::MAX, 65535_u64] => "ff0000000000000000003f0000",

	enum_unit: Key::Unit => "00",
	enum_newtype: Key::NewType("foo".to_string()) => "01666f6fffff",
	enum_tuple: Key::Tuple(false, vec![0x00, 0x01], u64::MAX) => "02010001ffff000000000000000000",
	enum_cow: Key::Cow(vec![0x00, 0x01].into(), false, String::from("foo").into()) => "030001ffff01666f6fffff",
	enum_cow_borrow: Key::Cow([0x00, 0x01].as_slice().into(), false, "foo".into()) => "030001ffff01666f6fffff",

	value_none: Value::none() => "00",
	value_bool: Value::Boolean(true) => "0100",
	value_float4: Value::Float4(OrderedF32::try_from(PI_F32).unwrap()) => "023fb6f024",
	value_float8: Value::Float8(OrderedF64::try_from(PI_F64).unwrap()) => "033ff6de04abbbd2e7",
	value_int1: Value::Int1(-1) => "0480",
	value_int4: Value::Int4(123456) => "067ffe1dbf",
	value_int8: Value::Int8(31415926) => "0701fffffffffe20a189",
	value_int16: Value::Int16(-123456789012345678901234567890i128) => "08800000018ee90ff6c373e0ee4e3f0ad1",
	value_string: Value::Utf8("foo".to_string()) => "09666f6fffff",
	value_uint1: Value::Uint1(255) => "0a00",
	value_uint2: Value::Uint2(65535) => "0b0000",
	value_uint4: Value::Uint4(4294967295) => "0c0f00000000",
	value_uint8: Value::Uint8(18446744073709551615) => "0d000000000000000000",
	value_uint16: Value::Uint16(340282366920938463463374607431768211455u128) => "0e00000000000000000000000000000000",

	// Option<bool>
	option_none_bool: None::<bool> => "00",
	option_some_true: Some(true) => "0100",
	option_some_false: Some(false) => "0101",

	// Option<f32>
	option_none_f32: None::<f32> => "00",
	option_some_f32: Some(PI_F32) => "013fb6f024",

	// Option<f64>
	option_none_f64: None::<f64> => "00",
	option_some_f64: Some(PI_F64) => "013ff6de04abbbd2e7",

	// Option<i8>
	option_none_i8: None::<i8> => "00",
	option_some_i8: Some(0i8) => "017f",

	// Option<i16>
	option_none_i16: None::<i16> => "00",
	option_some_i16: Some(0i16) => "017fff",

	// Option<i32>
	option_none_i32: None::<i32> => "00",
	option_some_i32: Some(0i32) => "017fffffff",

	// Option<i64>
	option_none_i64: None::<i64> => "00",
	option_some_i64: Some(0i64) => "017f",

	// Option<i128>
	option_none_i128: None::<i128> => "00",
	option_some_i128: Some(0i128) => "017fffffffffffffffffffffffffffffff",

	// Option<u8>
	option_none_u8: None::<u8> => "00",
	option_some_u8: Some(0u8) => "01ff",

	// Option<u16>
	option_none_u16: None::<u16> => "00",
	option_some_u16: Some(0u16) => "01ffff",

	// Option<u32>
	option_none_u32: None::<u32> => "00",
	option_some_u32: Some(0u32) => "01ff",

	// Option<u64>
	option_none_u64: None::<u64> => "00",
	option_some_u64: Some(0u64) => "01ff",

	// Option<u128>
	option_none_u128: None::<u128> => "00",
	option_some_u128: Some(0u128) => "01ffffffffffffffffffffffffffffffff",

	// Option<String>
	option_none_string: None::<String> => "00",
	option_some_string: Some("foo".to_string()) => "01666f6fffff",
	option_some_empty_string: Some("".to_string()) => "01ffff",

	// Option<ByteBuf>
	option_none_bytes: None::<ByteBuf> => "00",
	option_some_bytes: Some(ByteBuf::from(vec![0x01, 0xff])) => "0101ff00ffff",

	// Nested Option<Option<bool>>
	option_nested_none: None::<Option<bool>> => "00",
	option_nested_some_none: Some(None::<bool>) => "0100",
	option_nested_some_some_true: Some(Some(true)) => "010100",
	option_nested_some_some_false: Some(Some(false)) => "010101",

	// Nested Option<Option<i32>>
	option_nested_none_i32: None::<Option<i32>> => "00",
	option_nested_some_none_i32: Some(None::<i32>) => "0100",
	option_nested_some_some_i32: Some(Some(0i32)) => "01017fffffff",

	// Nested Option<Option<String>>
	option_nested_some_some_string: Some(Some("foo".to_string())) => "0101666f6fffff",

	// Triple nested Option<Option<Option<bool>>>
	option_triple_none: None::<Option<Option<bool>>> => "00",
	option_triple_some_none: Some(None::<Option<bool>>) => "0100",
	option_triple_some_some_none: Some(Some(None::<bool>)) => "010100",
	option_triple_some_some_some: Some(Some(Some(true))) => "01010100",}

	#[test]
	fn test_option_ordering() {
		// Descending: None > Some(MAX) > Some(0) > Some(MIN)
		// Byte order: None < Some(MAX) < Some(0) < Some(MIN)
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
		// Test bool
		let mut s = KeySerializer::new();
		s.extend_bool(true);
		assert_eq!(s.finish(), vec![0x00]);

		let mut s = KeySerializer::new();
		s.extend_bool(false);
		assert_eq!(s.finish(), vec![0x01]);

		// Test u64
		let mut s = KeySerializer::new();
		s.extend_u64(0u64);
		assert_eq!(s.finish(), vec![0xff]);

		// Test i64
		let mut s = KeySerializer::new();
		s.extend_i64(0i64);
		assert_eq!(s.finish(), vec![0x7f]);

		// Test f32
		let mut s = KeySerializer::new();
		s.extend_f32(0.0f32);
		assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff]);

		// Test f64
		let mut s = KeySerializer::new();
		s.extend_f64(0.0f64);
		assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

		// Test bytes
		let mut s = KeySerializer::new();
		s.extend_bytes(b"foo");
		assert_eq!(s.finish(), vec![0x66, 0x6f, 0x6f, 0xff, 0xff]);

		// Test chaining
		let mut s = KeySerializer::with_capacity(32);
		s.extend_bool(true).extend_u32(1u32).extend_i16(-1i16).extend_bytes(b"test");
		let result = s.finish();
		assert!(!result.is_empty());
		assert!(result.len() >= 10); // Should have all the encoded values
	}
}
