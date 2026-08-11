// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for extern-C procedures. Mirrors the operator ABI, specialised for procedure semantics (imperative,
//! may mutate).

pub mod descriptor;
pub mod types;
pub mod vtable;
