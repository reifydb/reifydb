// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use crate::hot::HotStorage;

#[derive(Clone)]
pub struct TransactionStoreConfig {
	pub hot: Option<HotConfig>,
	pub warm: Option<WarmConfig>,
	pub cold: Option<ColdConfig>,
	pub retention: RetentionConfig,
	pub merge_config: MergeConfig,
	pub stats: StorageStatsConfig,
}

/// Configuration for storage statistics tracking.
#[derive(Clone, Debug)]
pub struct StorageStatsConfig {
	/// Time between checkpoint persists.
	pub checkpoint_interval: Duration,
}

impl Default for StorageStatsConfig {
	fn default() -> Self {
		Self {
			checkpoint_interval: Duration::from_secs(10),
		}
	}
}

#[derive(Clone)]
pub struct HotConfig {
	pub storage: HotStorage,
	pub retention_period: Duration,
}

/// Warm tier configuration.
///
/// Placeholder for future warm tier configuration.
#[derive(Clone, Default)]
pub struct WarmConfig;

/// Cold tier configuration.
///
/// Placeholder for future cold tier configuration.
#[derive(Clone, Default)]
pub struct ColdConfig;

#[derive(Clone, Debug)]
pub struct RetentionConfig {
	pub hot: Duration,
	pub warm: Duration,
	// cold is forever (no eviction)
}

#[derive(Clone, Debug)]
pub struct MergeConfig {
	pub merge_threshold_rows: usize,
	pub merge_batch_size: usize,
	pub enable_auto_eviction: bool,
}

impl Default for TransactionStoreConfig {
	fn default() -> Self {
		Self {
			hot: None,
			warm: None,
			cold: None,
			retention: RetentionConfig {
				hot: Duration::from_secs(300),
				warm: Duration::from_secs(3600),
			},
			merge_config: MergeConfig {
				merge_threshold_rows: 100_000,
				merge_batch_size: 10_000,
				enable_auto_eviction: true,
			},
			stats: StorageStatsConfig::default(),
		}
	}
}

impl Default for RetentionConfig {
	fn default() -> Self {
		Self {
			hot: Duration::from_secs(300),   // 5 minutes
			warm: Duration::from_secs(3600), // 1 hour
		}
	}
}

impl Default for MergeConfig {
	fn default() -> Self {
		Self {
			merge_threshold_rows: 100_000,
			merge_batch_size: 10_000,
			enable_auto_eviction: true,
		}
	}
}
