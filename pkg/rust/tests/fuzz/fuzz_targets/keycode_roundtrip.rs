// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer};

#[derive(Arbitrary, Debug)]
struct RoundtripInput {
    b: bool,
    u1: u8,
    u2: u16,
    u4: u32,
    u8_val: u64,
    u16_val: u128,
    i1: i8,
    i2: i16,
    i4: i32,
    i8_val: i64,
    i16_val: i128,
    f4: f32,
    f8: f64,
    s: String,
}

fuzz_target!(|input: RoundtripInput| {
    let mut ser = KeySerializer::new();
    ser.extend_bool(input.b);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_bool().unwrap();
    assert_eq!(decoded, input.b);

    let mut ser = KeySerializer::new();
    ser.extend_u8(input.u1);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_u8().unwrap();
    assert_eq!(decoded, input.u1);

    let mut ser = KeySerializer::new();
    ser.extend_u16(input.u2);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_u16().unwrap();
    assert_eq!(decoded, input.u2);

    let mut ser = KeySerializer::new();
    ser.extend_u32(input.u4);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_u32().unwrap();
    assert_eq!(decoded, input.u4);

    let mut ser = KeySerializer::new();
    ser.extend_u64(input.u8_val);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_u64().unwrap();
    assert_eq!(decoded, input.u8_val);

    let mut ser = KeySerializer::new();
    ser.extend_u128(input.u16_val);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_u128().unwrap();
    assert_eq!(decoded, input.u16_val);

    let mut ser = KeySerializer::new();
    ser.extend_i8(input.i1);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_i8().unwrap();
    assert_eq!(decoded, input.i1);

    let mut ser = KeySerializer::new();
    ser.extend_i16(input.i2);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_i16().unwrap();
    assert_eq!(decoded, input.i2);

    let mut ser = KeySerializer::new();
    ser.extend_i32(input.i4);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_i32().unwrap();
    assert_eq!(decoded, input.i4);

    let mut ser = KeySerializer::new();
    ser.extend_i64(input.i8_val);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_i64().unwrap();
    assert_eq!(decoded, input.i8_val);

    let mut ser = KeySerializer::new();
    ser.extend_i128(input.i16_val);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_i128().unwrap();
    assert_eq!(decoded, input.i16_val);

    // Bit patterns, because a NaN never compares equal to itself.
    let mut ser = KeySerializer::new();
    ser.extend_f32(input.f4);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_f32().unwrap();
    assert_eq!(decoded.to_bits(), input.f4.to_bits());

    let mut ser = KeySerializer::new();
    ser.extend_f64(input.f8);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_f64().unwrap();
    assert_eq!(decoded.to_bits(), input.f8.to_bits());

    let mut ser = KeySerializer::new();
    ser.extend_str(&input.s);
    let bytes = ser.finish();
    let decoded = KeyDeserializer::from_bytes(&bytes).read_str().unwrap();
    assert_eq!(decoded, input.s);
});
