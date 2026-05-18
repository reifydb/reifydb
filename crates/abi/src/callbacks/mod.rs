// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Callback function pointers the host exports for the guest to invoke - catalog reads, RQL execution, store
//! access, logging, host-allocated memory, and per-extension state. The builder pattern lets the host assemble a
//! callback table piecemeal and hand a single complete struct to the guest at load time.

pub mod builder;
pub mod catalog;
pub mod host;
pub mod log;
pub mod memory;
pub mod rql;
pub mod state;
pub mod store;
