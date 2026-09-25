// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![no_main]

use libfuzzer_sys::fuzz_target;
use reifydb_codec::key::deserializer::KeyDeserializer;

fuzz_target!(|data: &[u8]| {
    let _ = KeyDeserializer::from_bytes(data).read_bool();
    let _ = KeyDeserializer::from_bytes(data).read_u8();
    let _ = KeyDeserializer::from_bytes(data).read_u16();
    let _ = KeyDeserializer::from_bytes(data).read_u32();
    let _ = KeyDeserializer::from_bytes(data).read_u64();
    let _ = KeyDeserializer::from_bytes(data).read_u128();
    let _ = KeyDeserializer::from_bytes(data).read_i8();
    let _ = KeyDeserializer::from_bytes(data).read_i16();
    let _ = KeyDeserializer::from_bytes(data).read_i32();
    let _ = KeyDeserializer::from_bytes(data).read_i64();
    let _ = KeyDeserializer::from_bytes(data).read_i128();
    let _ = KeyDeserializer::from_bytes(data).read_f32();
    let _ = KeyDeserializer::from_bytes(data).read_f64();
    let _ = KeyDeserializer::from_bytes(data).read_str();
});
