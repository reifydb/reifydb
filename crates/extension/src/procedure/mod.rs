// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod ffi;
pub mod ffi_callbacks;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod ffi_loader;
pub mod wasm;
pub mod wasm_loader;
