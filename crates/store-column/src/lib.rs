// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod backend;
pub mod config;
mod memory;
pub mod partition;
pub mod statistics;
mod store;

// New public exports
pub use backend::{Backend, ColumnBackend, MemoryColumnBackend};
pub use config::{BackendConfig, ColumnStoreConfig, CompressionConfig, RetentionConfig};
// Backward compatibility - alias the old MemoryColumnStore to the new backend
pub use memory::MemoryColumnStore;
pub use partition::{Partition, PartitionKey};
use reifydb_core::CommitVersion;
pub use store::StandardColumnStore;

// Convenience re-exports for backend modules
pub mod memory_backend {
	pub use crate::backend::memory::MemoryColumnBackend;
}

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	value::column::{ColumnData, CompressedColumn, CompressionType},
};
use reifydb_type::Value;

pub struct StoreColumnVersion;

impl HasVersion for StoreColumnVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-column".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Column-oriented storage for analytical queries".to_string(),
			r#type: ComponentType::Module,
		}
	}
}

/// Trait for columnar storage backends
pub trait ColumnStore: Send + Sync + Clone + 'static {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> reifydb_type::Result<()>;

	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> reifydb_type::Result<Vec<ColumnData>>;

	fn statistics(&self, column_index: usize) -> Option<ColumnStatistics>;

	fn partition_count(&self) -> usize;

	fn compressed_size(&self) -> usize;

	fn uncompressed_size(&self) -> usize;
}

#[derive(Clone, Debug)]
pub struct ColumnStatistics {
	pub min_value: Option<Value>,
	pub max_value: Option<Value>,
	pub undefined_count: usize,
	pub distinct_count: Option<usize>,
	pub compression_type: CompressionType,
	pub compression_ratio: f64,
	pub compressed_size: usize,
	pub uncompressed_size: usize,
}
