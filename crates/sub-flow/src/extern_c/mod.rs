// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Host-side extern-C surface for flow operators: the callbacks a guest extension invokes and the
//! per-call context wrapping the engine services it may reach. Symbol shape is fixed by `reifydb-abi`.

pub mod callbacks;
pub mod context;
