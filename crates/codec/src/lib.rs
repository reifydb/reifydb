// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Every byte layout that crosses a ReifyDB boundary: the type-tag namespace, the self-describing
//! value codec, the RBCF columnar frame format, the row storage codec, the key codec and the FFI
//! cells. Both halves live here and share one tag scheme, so no consumer hand-rolls these bytes.

pub mod constraint;
pub mod error;
pub mod ffi;
pub mod frame;
#[cfg(feature = "json")]
pub mod json;
pub mod key;
pub mod primitive;
pub mod reader;
pub mod row;
pub mod tag;
pub mod typeinfo;
pub mod value;
