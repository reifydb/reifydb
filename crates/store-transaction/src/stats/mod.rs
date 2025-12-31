// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage statistics tracking for real-time monitoring of storage consumption.
//!
//! This module provides:
//! - Per-object-type tracking (tables, views, indexes, CDC, etc.)
//! - Per-individual-object tracking (specific tables, specific indexes)
//! - MVCC-aware breakdown (current vs historical versions)
//! - Per-tier tracking (hot/warm/cold)

mod parser;
pub mod persistence;
mod query;
mod tracker;
mod types;

pub use tracker::{PreVersionInfo, StorageTracker, StorageTrackerConfig};
pub use types::{ObjectId, StorageStats, Tier, TierStats};
