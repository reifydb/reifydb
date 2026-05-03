// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Host-side FFI for flow operators. Provides the callback functions guest extensions invoke (catalog reads, value
//! marshalling, error reporting) and the per-call context that wraps the engine services available to a guest
//! operator. The shape of these symbols is fixed by `reifydb-abi`; the implementation lives here.

pub mod callbacks;
pub mod context;
