// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for FFI operators. Defines the descriptor an operator declares about itself, the column types it
//! sees on the boundary, the capabilities flags the host inspects to know what an operator supports, and the
//! vtable that gives the host concrete function pointers to invoke. Every field is `repr(C)` and wire-stable.

pub mod capabilities;
pub mod column;
pub mod descriptor;
pub mod types;
pub mod vtable;
