// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Canonical byte layouts for every primitive the database stores or transmits, one submodule per
//! kind. A shipped layout is used by storage, replication, CDC and the wire protocol at once, so
//! changing one is a coordinated cross-format break: old bytes must keep round-tripping.

pub mod any;
pub mod blob;
pub mod boolean;
pub mod bytes;
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
pub mod le;
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
