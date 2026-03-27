// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod identity_inject;
#[cfg(reifydb_target = "native")]
pub mod loader;
pub mod wasm;
pub mod wasm_loader;
