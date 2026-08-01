// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use reifydb_codec::key as keycode;

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
    let encoded = keycode::serialize(&input.b);
    let decoded: bool = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.b);

    let encoded = keycode::serialize(&input.u1);
    let decoded: u8 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.u1);

    let encoded = keycode::serialize(&input.u2);
    let decoded: u16 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.u2);

    let encoded = keycode::serialize(&input.u4);
    let decoded: u32 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.u4);

    let encoded = keycode::serialize(&input.u8_val);
    let decoded: u64 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.u8_val);

    let encoded = keycode::serialize(&input.u16_val);
    let decoded: u128 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.u16_val);

    let encoded = keycode::serialize(&input.i1);
    let decoded: i8 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.i1);

    let encoded = keycode::serialize(&input.i2);
    let decoded: i16 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.i2);

    let encoded = keycode::serialize(&input.i4);
    let decoded: i32 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.i4);

    let encoded = keycode::serialize(&input.i8_val);
    let decoded: i64 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.i8_val);

    let encoded = keycode::serialize(&input.i16_val);
    let decoded: i128 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.i16_val);

    // Bit patterns, because a NaN never compares equal to itself.
    let encoded = keycode::serialize(&input.f4);
    let decoded: f32 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded.to_bits(), input.f4.to_bits());

    let encoded = keycode::serialize(&input.f8);
    let decoded: f64 = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded.to_bits(), input.f8.to_bits());

    let encoded = keycode::serialize(&input.s);
    let decoded: String = keycode::deserialize(&encoded).unwrap();
    assert_eq!(decoded, input.s);
});
