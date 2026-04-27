// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Deferred (CDC-driven) view processing subsystem.
//!
//! Deferred views are updated asynchronously: writes commit first, then CDC events
//! trigger view updates in background workers.

pub(crate) mod coordinator;
pub(crate) mod pool;
pub(crate) mod state;
pub(crate) mod tracker;
pub(crate) mod watermark;
pub(crate) mod worker;
