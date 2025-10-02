// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use crate::backend::Backend;

#[derive(Clone)]
pub struct ColumnStoreConfig {
	pub hot: Option<BackendConfig>,
	pub warm: Option<BackendConfig>,
	pub cold: Option<BackendConfig>,
	pub retention: RetentionConfig,
	pub compression: CompressionConfig,
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
pub struct CompressionConfig {
	pub hot_compression_level: u8,  // Lighter compression for fast access
	pub warm_compression_level: u8, // Medium compression
	pub cold_compression_level: u8, // Heavy compression for space efficiency
	pub enable_dictionary_compression: bool,
	pub enable_delta_compression: bool,
	pub enable_rle_compression: bool,
}

impl Default for ColumnStoreConfig {
	fn default() -> Self {
		Self {
			hot: None,
			warm: None,
			cold: None,
			retention: RetentionConfig::default(),
			compression: CompressionConfig::default(),
		}
	}
}

impl Default for RetentionConfig {
	fn default() -> Self {
		Self {
			hot: Duration::from_secs(1800),  // 30 minutes
			warm: Duration::from_secs(7200), // 2 hours
		}
	}
}

impl Default for CompressionConfig {
	fn default() -> Self {
		Self {
			hot_compression_level: 1,  // Light compression for speed
			warm_compression_level: 3, // Balanced compression
			cold_compression_level: 6, // Heavy compression for storage
			enable_dictionary_compression: true,
			enable_delta_compression: true,
			enable_rle_compression: true,
		}
	}
}
