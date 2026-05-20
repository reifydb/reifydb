// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod ffi_callbacks;
#[cfg(reifydb_target = "native")]
pub mod ffi_loader;
pub mod wasm;
pub mod wasm_loader;
