// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shapes and helpers shared by more than one extension stage. Anything reachable from only one of flow,
//! transform or procedure belongs in that stage's own tree instead.

pub mod extern_c;
pub mod extern_wasm;
