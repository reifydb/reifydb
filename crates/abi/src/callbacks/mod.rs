// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Callback function pointers the host exports for the guest to invoke. The builder lets the host assemble the
//! table piecemeal and hand the guest one complete struct at load time.

pub mod builder;
pub mod catalog;
pub mod dictionary;
pub mod host;
pub mod log;
pub mod memory;
pub mod rql;
pub mod state;
pub mod store;
