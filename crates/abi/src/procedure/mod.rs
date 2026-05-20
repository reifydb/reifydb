// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for FFI procedures: the descriptor (name, signature, capabilities), the typed argument and
//! return shapes, and the vtable of function pointers the host calls into. Same shape as the operator ABI but
//! specialised for the procedure semantics (imperative, may mutate).

pub mod descriptor;
pub mod types;
pub mod vtable;
