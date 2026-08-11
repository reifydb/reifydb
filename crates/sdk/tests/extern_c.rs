// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "extern_c/common.rs"]
mod common;

#[path = "extern_c/bool.rs"]
mod bool_t;

#[path = "extern_c/float4.rs"]
mod float4;

#[path = "extern_c/float8.rs"]
mod float8;

#[path = "extern_c/int1.rs"]
mod int1;

#[path = "extern_c/int2.rs"]
mod int2;

#[path = "extern_c/int4.rs"]
mod int4;

#[path = "extern_c/int8.rs"]
mod int8;

#[path = "extern_c/int16.rs"]
mod int16;

#[path = "extern_c/uint1.rs"]
mod uint1;

#[path = "extern_c/uint2.rs"]
mod uint2;

#[path = "extern_c/uint4.rs"]
mod uint4;

#[path = "extern_c/uint8.rs"]
mod uint8;

#[path = "extern_c/uint16.rs"]
mod uint16;

#[path = "extern_c/utf8.rs"]
mod utf8;

#[path = "extern_c/blob.rs"]
mod blob;

#[path = "extern_c/date.rs"]
mod date;

#[path = "extern_c/datetime.rs"]
mod datetime;

#[path = "extern_c/time.rs"]
mod time;

#[path = "extern_c/duration.rs"]
mod duration;

#[path = "extern_c/identity.rs"]
mod identity;

#[path = "extern_c/uuid4.rs"]
mod uuid4;

#[path = "extern_c/uuid7.rs"]
mod uuid7;

#[path = "extern_c/dictionary.rs"]
mod dictionary;

#[path = "extern_c/bigint.rs"]
mod bigint;

#[path = "extern_c/biguint.rs"]
mod biguint;

#[path = "extern_c/decimal.rs"]
mod decimal;

#[path = "extern_c/any.rs"]
mod any;

#[path = "extern_c/option.rs"]
mod option;
