// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Columnar payload shapes that cross the FFI boundary. Buffer, column, and layout cover the host-WASM-capable
//! representation of typed data; the WASM submodule carries the variant that runs entirely inside a sandboxed
//! guest where pointer widths and ABI conventions differ from the native side.

pub mod buffer;
pub mod column;
pub mod layout;
pub mod wasm;
