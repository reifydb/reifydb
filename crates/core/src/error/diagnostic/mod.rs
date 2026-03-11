// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Database-level diagnostic error modules.
//!
//! These diagnostics deal with database constructs like catalogs, indexes,
//! transactions, etc. Type-level diagnostics remain in reifydb-type.

pub mod catalog;
pub mod core_error;
pub mod engine;
pub mod flow;
pub mod index;
pub mod internal;
pub mod operation;
pub mod query;
pub mod sequence;
pub mod subscription;
pub mod subsystem;
pub mod transaction;
