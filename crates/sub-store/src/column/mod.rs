// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Storage-materialization subsystem: actors that turn committed rows into materialized columns, write the
//! `ColumnSnapshot` catalog rows, and populate the `ColumnBlockStore`. The tick policy owns the trade-off
//! between materialization latency and write amplification.

pub mod actor;
pub mod block_store;
pub mod error;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod persistent;
