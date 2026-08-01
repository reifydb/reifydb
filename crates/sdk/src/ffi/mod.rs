// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Guest-side FFI: the symbols a cdylib extension exports so the host can call into it, and the arena that owns
//! any memory handed back across the boundary. Layout here mirrors `reifydb-abi` exactly, so adding, removing or
//! resizing an exported symbol requires a matching change on the host loader side.

pub mod arena;
pub mod exports;
pub mod wrapper;
