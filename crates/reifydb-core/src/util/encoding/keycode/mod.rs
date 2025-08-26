// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

mod deserialize;
mod serialize;

use serde::{Deserialize, Serialize};

use crate::{
	error::diagnostic::serialization,
	util::encoding::keycode::{
		deserialize::Deserializer, serialize::Serializer,
	},
};

#[macro_export]
macro_rules! key_prefix {
    ($($arg:tt)*) => {
        &EncodedKey::new((&format!($($arg)*)).as_bytes().to_vec())
    };
}

/// Serializes a key to a binary Keycode representation (Descending order)
pub fn serialize<T: Serialize>(key: &T) -> Vec<u8> {
	let mut serializer = Serializer {
		output: Vec::new(),
	};
	// Panic on failure, as this is a problem with the data structure.
	key.serialize(&mut serializer).expect("key must be serializable");
	serializer.output
}

/// Deserializes a key from a binary Keycode representation (Descending order)
pub fn deserialize<'a, T: Deserialize<'a>>(
	input: &'a [u8],
) -> crate::Result<T> {
	let mut deserializer = Deserializer::from_bytes(input);
	let t = T::deserialize(&mut deserializer)?;
	if !deserializer.input.is_empty() {
		return Err(crate::error!(
			serialization::keycode_serialization_error(format!(
				"unexpected trailing bytes {:x?} at end of key {input:x?}",
				deserializer.input,
			))
		));
	}
	Ok(t)
}

#[cfg(test)]
mod tests {
	use std::{
		borrow::Cow, f32::consts::PI as PIf32, f64::consts::PI as PIf64,
	};

	use serde::{Deserialize, Serialize};
	use serde_bytes::ByteBuf;

	use super::*;
	use crate::{
		Value,
		value::{OrderedF32, OrderedF64},
	};

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
                assert_eq!(crate::util::hex::encode(&output), expect, "encode failed");

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
	    f32_neg_pi: -PIf32 => "c0490fdb",
	    f32_neg_zero: -0f32 => "80000000",
	    f32_zero: 0f32 => "7fffffff",
	    f32_pi: PIf32 => "3fb6f024",
	    f32_max: f32::MAX => "00800000",
	    f32_inf: f32::INFINITY => "007fffff",

	    f64_min: f64::MIN => "ffefffffffffffff",
	    f64_neg_inf: f64::NEG_INFINITY => "fff0000000000000",
	    f64_neg_pi: -PIf64 => "c00921fb54442d18",
	    f64_neg_zero: -0f64 => "8000000000000000",
	    f64_zero: 0f64 => "7fffffffffffffff",
	    f64_pi: PIf64 => "3ff6de04abbbd2e7",
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

	    value_undefined: Value::Undefined => "00",
	    value_bool: Value::Bool(true) => "0100",
	    value_float4: Value::Float4(OrderedF32::try_from(PIf32).unwrap()) => "023fb6f024",
	    value_float8: Value::Float8(OrderedF64::try_from(PIf64).unwrap()) => "033ff6de04abbbd2e7",
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
	}
}
