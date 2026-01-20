// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage and CDC metrics tracking for ReifyDB.
//!
//! This crate provides:
//! - Per-object MVCC storage statistics with tier breakdown
//! - Per-object CDC storage statistics (no tiering)
//! - Single-writer background worker for stats processing
//! - Read-only interfaces for querying stats
//!
//! # Architecture
//!
//! The metrics system follows a single-writer pattern:
//! - [`MetricsWorker`] is the only component that writes stats
//! - [`StorageStatsReader`] and [`CdcStatsReader`] provide read-only access
//! - Stats are persisted using SingleVersion storage traits from `reifydb-core`
//!
//! # Usage
//!
//! ```ignore
//! // Create worker (single writer)
//! let worker = MetricsWorker::new(config, storage.clone(), event_bus);
//!
//! // Queue stats operations
//! worker.record_multi(vec![
//!     MultiStorageOperation::Write { tier: Tier::Hot, key: key.clone(), value_bytes: 100 },
//! ], version);
//!
//! // Read stats
//! let reader = StorageStatsReader::new(storage);
//! let stats = reader.get(Tier::Hot, id)?;
//! ```

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId};

pub mod cdc;
pub mod encoding;
pub mod metric;
pub mod multi;
pub mod parser;
pub mod worker;

/// Identifier for tracking per-object storage statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricId {
	/// Table, view, or flow source
	Source(PrimitiveId),
	/// Flow operator node
	FlowNode(FlowNodeId),
	/// System metadata (sequences, versions, etc.)
	System,
}
