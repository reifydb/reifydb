// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(target_arch = "wasm32"))]
pub mod cdc;
pub mod constraint;
pub mod error;
pub mod extern_c;
pub mod frame;
#[cfg(feature = "json")]
pub mod json;
pub mod key;
pub mod log;
pub mod primitive;
pub mod reader;
pub mod row;
pub mod tag;
pub mod typeinfo;
pub mod value;
