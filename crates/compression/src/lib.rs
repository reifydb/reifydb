// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	value::column::{compressed::CompressedColumn, data::ColumnData},
};

pub mod select;
pub mod strategy;

pub type BoxedColumnCompressor = Box<dyn ColumnCompressor>;

pub trait ColumnCompressor: Send + Sync {
	fn compress(&self, data: &ColumnData) -> reifydb_type::Result<CompressedColumn>;
	fn decompress(&self, compressed: &CompressedColumn) -> reifydb_type::Result<ColumnData>;
}

/// Statistics collected during compression
#[derive(Clone, Debug)]
pub struct CompressionStatistics {
	pub original_size: usize,
	pub compressed_size: usize,
	pub compression_time_ms: u64,
	pub compression_ratio: f64,
}

pub struct CompressionVersion;

impl HasVersion for CompressionVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME").strip_prefix("reifydb-").unwrap_or(env!("CARGO_PKG_NAME")).to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Column compression for storage and network".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
