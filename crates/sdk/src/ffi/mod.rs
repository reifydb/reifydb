// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Guest-side FFI: the symbols a built-as-cdylib extension exports so the host can find and call into it. The
//! arena owns memory the extension hands back across the boundary; the exports module declares the C-callable
//! entry points; the wrappers turn the resulting C ABI into the typed Rust surface the rest of `sdk/` exposes.
//!
//! Layout invariants here mirror `reifydb-abi` exactly. Anything that adds, removes, or resizes an exported
//! symbol must be matched by a coordinated change on the host loader side.

pub mod arena;
pub mod exports;
pub mod wrapper;
