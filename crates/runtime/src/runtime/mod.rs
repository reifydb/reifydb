// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Runtime implementations for different platforms.

// Re-exports for internal use
#[cfg(reifydb_target = "native")]
pub mod native;
#[cfg(reifydb_target = "wasm")]
pub mod wasm;

cfg_if::cfg_if! {
    if #[cfg(reifydb_target = "native")] {
        pub(crate) use native::NativeRuntime;
    } else {
        pub(crate) use wasm::{WasmHandle, WasmJoinError, WasmJoinHandle, WasmRuntime};
    }
}
