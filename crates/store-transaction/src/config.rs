// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use crate::backend::Backend;

#[derive(Clone)]
pub struct TransactionStoreConfig {
	pub hot: Option<BackendConfig>,
	pub warm: Option<BackendConfig>,
	pub cold: Option<BackendConfig>,
	pub retention: RetentionConfig,
	pub merge_config: MergeConfig,
}

#[derive(Clone)]
pub struct BackendConfig {
	pub backend: Backend,
	pub retention_period: Duration,
}

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
