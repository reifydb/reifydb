// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Columnar payload shapes crossing the FFI boundary. The `wasm` submodule carries the sandboxed-guest variant,
//! where pointer width and ABI conventions differ from the native side.

pub mod buffer;
pub mod column;
pub mod constraint;
pub mod key_ref;
pub mod state;
pub mod wasm;
