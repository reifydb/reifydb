// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Result, Value};

use crate::{
	CommitVersion,
	value::column::{ColumnData, CompressedColumn, CompressionType},
};

/// Trait for columnar storage backends
pub trait ColumnStore: Send + Sync + Clone + 'static {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()>;

	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>>;

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
