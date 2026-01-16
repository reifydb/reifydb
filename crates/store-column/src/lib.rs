// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod backend;
pub mod config;
pub mod memory;
pub mod partition;
pub mod statistics;
pub mod store;

use partition::{Partition, PartitionKey};
use reifydb_core::common::CommitVersion;

pub mod memory_backend {}

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	value::column::{
		compressed::{CompressedColumn, CompressionType},
		data::ColumnData,
	},
};
use reifydb_type::value::Value;

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
