// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	value::column::{ColumnData, CompressedColumn},
};

mod select;
mod strategy;

pub use select::select_compressor;

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
			name: "compression".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Column compression for storage and network".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
