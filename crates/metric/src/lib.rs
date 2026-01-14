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

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::{FlowNodeId, PrimitiveId};

pub mod cdc;
mod encoding;
mod metric;
pub mod multi;
mod parser;
mod worker;

/// Identifier for tracking per-object storage statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Id {
	/// Table, view, or flow source
	Source(PrimitiveId),
	/// Flow operator node
	FlowNode(FlowNodeId),
	/// System metadata (sequences, versions, etc.)
	System,
}

// Re-export from cdc
pub use cdc::{CdcOperation, CdcStats, CdcStatsReader, CdcStatsWriter};

// Re-export from multi
pub use multi::{MultiStorageOperation, MultiStorageStats, StorageStatsReader, StorageStatsWriter, Tier, TieredStorageStats};

// Re-export from metric
pub use metric::{CombinedStats, MetricReader};

// Re-export from worker
pub use worker::{CdcStatsListener, MetricsEvent, MetricsWorker, MetricsWorkerConfig, StorageStatsListener};

// Re-export encoding functions (for external use)
pub use encoding::{
	cdc_stats_key_prefix, decode_cdc_stats, decode_cdc_stats_key, decode_storage_stats, decode_storage_stats_key,
	decode_type_stats_key, encode_cdc_stats, encode_cdc_stats_key, encode_storage_stats, encode_storage_stats_key,
	encode_type_stats_key, storage_stats_key_prefix, type_stats_key_prefix, CDC_STATS_SIZE, STORAGE_STATS_SIZE,
};

// Re-export parser
pub use parser::parse_id;
