// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Session management for ReifyDB.
//!
//! Re-exports the unified `Session` type from the engine crate.

pub use reifydb_engine::session::{Backoff, RetryPolicy, Session};
