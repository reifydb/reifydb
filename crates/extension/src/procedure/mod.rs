// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod ffi_callbacks;
#[cfg(reifydb_target = "native")]
pub mod ffi_loader;
pub mod wasm;
pub mod wasm_loader;
