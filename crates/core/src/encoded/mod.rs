// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Canonical on-disk and on-wire byte layouts for every primitive value the database stores or transmits.
//!
//! There is one submodule per primitive kind: signed and unsigned integers from 8 to 128 bits, IEEE floats, booleans,
//! variable-length blobs and UTF-8 strings, decimals, the temporal family (date, datetime, time, duration), the UUID
//! variants, identity and dictionary references, the arbitrary-precision integer wrappers, the typed-row and shape
//! envelopes, the typed-key encoding, the `any`-tagged variant for heterogeneous columns, and the `undefined` sentinel.
//! Each submodule supplies the encode and decode routines that take a typed value to the bytes that go to disk,
//! replication, CDC, and the wire protocol, and back again.
//!
//! Invariant: once a primitive's byte layout has shipped, it is the format used by storage, replication, CDC, and the
//! wire protocol simultaneously. Any change is a coordinated cross-format break that requires a migration; consumers
//! must continue to round-trip every previously-written byte sequence forever.

pub mod any;
pub mod blob;
pub mod boolean;
pub mod date;
pub mod datetime;
pub mod decimal;
pub mod dictionary_id;
pub mod duration;
pub mod f32;
pub mod f64;
pub mod i128;
pub mod i16;
pub mod i32;
pub mod i64;
pub mod i8;
pub mod identity;
pub mod int;
pub mod key;
pub mod row;
pub mod shape;
pub mod time;
pub mod u128;
pub mod u16;
pub mod u32;
pub mod u64;
pub mod u8;
pub mod uint;
pub mod undefined;
pub mod utf8;
pub mod uuid4;
pub mod uuid7;
pub mod value;
