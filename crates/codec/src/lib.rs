// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Unified encoding and decoding for ReifyDB. Every byte layout that crosses a boundary lives here:
//! the single type-tag namespace, the canonical self-describing value codec, the RBCF columnar frame
//! format, the row storage codec, the order-preserving key codec, and the FFI cell codecs.
//!
//! Invariant: encode and decode halves are colocated in this crate and share one tag scheme. A tag,
//! layout, or width change is a coordinated workspace change; no consumer may hand-roll these bytes.

pub mod constraint;
pub mod encoded;
pub mod error;
pub mod ffi;
pub mod frame;
#[cfg(feature = "json")]
pub mod json;
pub mod key;
pub mod reader;
pub mod tag;
pub mod typeinfo;
pub mod value;
