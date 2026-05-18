// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for FFI procedures: the descriptor (name, signature, capabilities), the typed argument and
//! return shapes, and the vtable of function pointers the host calls into. Same shape as the operator ABI but
//! specialised for the procedure semantics (imperative, may mutate).

pub mod descriptor;
pub mod types;
pub mod vtable;
