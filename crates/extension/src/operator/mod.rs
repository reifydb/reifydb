// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Operator extension points (FFI, WASM)
//!
//! Operators are stateful flow-graph nodes that process changes (insert/update/delete).
//! Unlike transforms, operators maintain state across invocations via apply/pull/tick lifecycle.

#[cfg(reifydb_target = "native")]
pub mod ffi_loader;
