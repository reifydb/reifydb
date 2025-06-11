// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
//! Keycode is a lexicographical order-preserving binary encoding for use with
//! keys in key/value stores. It is designed for simplicity, not efficiency
//! (i.e. it does not use varints or other compression methods).
//!
//! Ordering is important because it allows limited scans across specific parts
//! of the keyspace, e.g. scanning an individual table or using an index range
//! predicate like `WHERE id < 100`
//!
//! The encoding is not self-describing: the caller must provide a concrete type
//! to decode into, and the binary key must conform to its structure.
//!
//! Keycode supports a subset of primitive data types, encoded as follows:
//!
//! * [`bool`]: `0x00` for `false`, `0x01` for `true`.
//! * [`u64`]: big-endian binary representation.
//! * [`i64`]: big-endian binary, sign bit flipped.
//! * [`f64`]: big-endian binary, sign bit flipped, all flipped if negative.
//! * [`Vec<u8>`]: `0x00` escaped as `0x00ff`, terminated with `0x0000`.
//! * [`String`]: like [`Vec<u8>`].
//! * Sequences: concatenation of contained elements, with no other structure.
//! * Enum: the variant's index as [`u8`], then the content sequence.
//! * [`crate::sql::types::Value`]: like any other enum.
//!
//! The canonical key representation is an enum. For example:
//!
//! ```
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! enum Key {
//!     Foo,
//!     Bar(String),
//!     Baz(bool, u64, #[serde(with = "serde_bytes")] Vec<u8>),
//! }
//! ```
//!
//! Unfortunately, byte strings such as `Vec<u8>` must be wrapped with
//! [`serde_bytes::ByteBuf`] or use the `#[serde(with="serde_bytes")]`
//! attribute. See <https://github.com/serde-rs/bytes>.

mod deserialize;
mod error;
mod serialize;

use crate::encoding::Error;
use crate::encoding::keycode::deserialize::Deserializer;
use crate::encoding::keycode::serialize::Serializer;
use crate::{encoding, invalid_data};
use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! key_prefix {
    ($($arg:tt)*) => {
        &EncodedKey::new((&format!($($arg)*)).as_bytes().to_vec())
    };
}

/// Serializes a key to a binary Keycode representation.
///
/// In the common case, the encoded key is borrowed for a  call
/// and then thrown away. We could avoid a bunch of allocations by taking a
/// reusable byte vector to encode into and return a reference to it, but we
/// keep it simple.
pub fn serialize<T: Serialize>(key: &T) -> Vec<u8> {
    let mut serializer = Serializer { output: Vec::new() };
    // Panic on failure, as this is a problem with the data structure.
    key.serialize(&mut serializer).expect("key must be serializable");
    serializer.output
}

/// Deserializes a key from a binary Keycode representation.
pub fn deserialize<'a, T: Deserialize<'a>>(input: &'a [u8]) -> encoding::Result<T> {
    let mut deserializer = Deserializer::from_bytes(input);
    let t = T::deserialize(&mut deserializer)?;
    if !deserializer.input.is_empty() {
        return invalid_data!(
            "unexpected trailing bytes {:x?} at end of key {input:x?}",
            deserializer.input,
        );
    }
    Ok(t)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::f32::consts::PI as PIf32;
    use std::f64::consts::PI as PIf64;

    use super::*;
    use crate::Value;
    use crate::ordered_float::{OrderedF32, OrderedF64};
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;

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

    /// Assert that serializing a value yields the expected byte sequence (as a
    /// hex-encoded string), and that deserializing it yields the original value.
    macro_rules! test_serde {
        ( $( $name:ident: $input:expr => $expect:literal, )* ) => {
        $(
            #[test]
            fn $name(){
                let mut input = $input;
                let expect = $expect;
                let output = serialize(&input);
                assert_eq!(hex::encode(&output), expect, "encode failed");

                let expect = input;
                input = deserialize(&output).unwrap(); // reuse input variable for proper type
                assert_eq!(input, expect, "decode failed");
            }
        )*
        };
    }

    test_serde! {
        bool_false: false => "00",
        bool_true: true => "01",

        f32_min: f32::MIN => "00800000",
        f32_neg_inf: f32::NEG_INFINITY => "007fffff",
        f32_neg_pi: -PIf32 => "3fb6f024",
        f32_neg_zero: -0f32 => "7fffffff",
        f32_zero: 0f32 => "80000000",
        f32_pi: PIf32 => "c0490fdb",
        f32_max: f32::MAX => "ff7fffff",
        f32_inf: f32::INFINITY => "ff800000",
        // We don't test NAN here, since NAN != NAN.

        f64_min: f64::MIN => "0010000000000000",
        f64_neg_inf: f64::NEG_INFINITY => "000fffffffffffff",
        f64_neg_pi: -PIf64 => "3ff6de04abbbd2e7",
        f64_neg_zero: -0f64 => "7fffffffffffffff",
        f64_zero: 0f64 => "8000000000000000",
        f64_pi: PIf64 => "c00921fb54442d18",
        f64_max: f64::MAX => "ffefffffffffffff",
        f64_inf: f64::INFINITY => "fff0000000000000",
        // We don't test NAN here, since NAN != NAN.

        i8_min: i8::MIN => "00",
        i8_neg_1: -1i8 => "7f",
        i8_0: 0i8 => "80",
        i8_1: 1i8 => "81",
        i8_max: i8::MAX => "ff",

        i16_min: i16::MIN => "0000",
        i16_neg_1: -1i16 => "7fff",
        i16_0: 0i16 => "8000",
        i16_1: 1i16 => "8001",
        i16_max: i16::MAX => "ffff",

        i32_min: i32::MIN => "00000000",
        i32_neg_1: -1i32 => "7fffffff",
        i32_0: 0i32 => "80000000",
        i32_1: 1i32 => "80000001",
        i32_max: i32::MAX => "ffffffff",

        i64_min: i64::MIN => "0000000000000000",
        i64_neg_65535: -65535i64 => "7fffffffffff0001",
        i64_neg_1: -1i64 => "7fffffffffffffff",
        i64_0: 0i64 => "8000000000000000",
        i64_1: 1i64 => "8000000000000001",
        i64_65535: 65535i64 => "800000000000ffff",
        i64_max: i64::MAX => "ffffffffffffffff",

        i128_min: i128::MIN => "00000000000000000000000000000000",
        i128_neg_1: -1i128 => "7fffffffffffffffffffffffffffffff",
        i128_0: 0i128 => "80000000000000000000000000000000",
        i128_1: 1i128 => "80000000000000000000000000000001",
        i128_max: i128::MAX => "ffffffffffffffffffffffffffffffff",

        u8_min: u8::MIN => "00",
        u8_1: 1_u8 => "01",
        u8_255: 255_u8 => "ff",

        u16_min: u16::MIN => "0000",
        u16_1: 1_u16 => "0001",
        u16_255: 255_u16 => "00ff",
        u16_65535: u16::MAX => "ffff",

        u32_min: u32::MIN => "00000000",
        u32_1: 1_u32 => "00000001",
        u32_65535: 65535_u32 => "0000ffff",
        u32_max: u32::MAX => "ffffffff",

        u64_min: u64::MIN => "0000000000000000",
        u64_1: 1_u64 => "0000000000000001",
        u64_65535: 65535_u64 => "000000000000ffff",
        u64_max: u64::MAX => "ffffffffffffffff",

        u128_min: u128::MIN => "00000000000000000000000000000000",
        u128_1: 1_u128 => "00000000000000000000000000000001",
        u128_65535: 65535_u128 => "0000000000000000000000000000ffff",
        u128_max: u128::MAX => "ffffffffffffffffffffffffffffffff",

        bytes: ByteBuf::from(vec![0x01, 0xff]) => "01ff0000",
        bytes_empty: ByteBuf::new() => "0000",
        bytes_escape: ByteBuf::from(vec![0x00, 0x01, 0x02]) => "00ff01020000",

        string: "foo".to_string() => "666f6f0000",
        string_empty: "".to_string() => "0000",
        string_escape: "foo\x00bar".to_string() => "666f6f00ff6261720000",
        string_utf8: "👋".to_string() => "f09f918b0000",

        tuple: (true, u64::MAX, ByteBuf::from(vec![0x00, 0x01])) => "01ffffffffffffffff00ff010000",
        array_bool: [false, true, false] => "000100",
        vec_bool: vec![false, true, false] => "000100",
        vec_u64: vec![u64::MIN, u64::MAX, 65535_u64] => "0000000000000000ffffffffffffffff000000000000ffff",

        enum_unit: Key::Unit => "00",
        enum_newtype: Key::NewType("foo".to_string()) => "01666f6f0000",
        enum_tuple: Key::Tuple(false, vec![0x00, 0x01], u64::MAX) => "020000ff010000ffffffffffffffff",
        enum_cow: Key::Cow(vec![0x00, 0x01].into(), false, String::from("foo").into()) => "0300ff01000000666f6f0000",
        enum_cow_borrow: Key::Cow([0x00, 0x01].as_slice().into(), false, "foo".into()) => "0300ff01000000666f6f0000",

        value_undefined: Value::Undefined => "00",
        value_bool: Value::Bool(true) => "0101",
        value_int1: Value::Int1(-1) => "047f",
        value_float4: Value::Float4(OrderedF32::try_from(PIf32).unwrap()) => "02c0490fdb",
        value_float8: Value::Float8(OrderedF64::try_from(PIf64).unwrap()) => "03c00921fb54442d18",
        value_int4: Value::Int4(123456) => "068001e240",
        value_int8: Value::Int8(31415926) => "078000000001df5e76",
        value_int16: Value::Int16(-123456789012345678901234567890i128) => "087ffffffe7116f0093c8c1f11b1c0f52e",
        value_string: Value::String("foo".to_string()) => "09666f6f0000",
        value_uint1: Value::Uint1(255) => "0aff",
        value_uint2: Value::Uint2(65535) => "0bffff",
        value_uint4: Value::Uint4(4294967295) => "0cffffffff",
        value_uint8: Value::Uint8(18446744073709551615) => "0dffffffffffffffff",
        value_uint16: Value::Uint16(340282366920938463463374607431768211455u128) => "0effffffffffffffffffffffffffffffff",
    }
}
