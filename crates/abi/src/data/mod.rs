// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Columnar payload shapes that cross the FFI boundary. Buffer, column, and layout cover the host-WASM-capable
//! representation of typed data; the WASM submodule carries the variant that runs entirely inside a sandboxed
//! guest where pointer widths and ABI conventions differ from the native side.

pub mod buffer;
pub mod column;
pub mod key_ref;
pub mod layout;
pub mod wasm;
