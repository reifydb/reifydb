// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Runtime implementations for different platforms.

// Re-exports for internal use
#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "wasm")]
pub mod wasm;

cfg_if::cfg_if! {
    if #[cfg(feature = "native")] {
        pub(crate) use native::NativeRuntime;
    } else if #[cfg(feature = "wasm")] {
        pub(crate) use wasm::{WasmHandle, WasmJoinError, WasmJoinHandle, WasmRuntime};
    }
}
