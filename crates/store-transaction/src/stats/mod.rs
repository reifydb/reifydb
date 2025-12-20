// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
