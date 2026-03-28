// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	common::CommitVersion,
	value::column::{compressed::CompressedColumn, data::ColumnData},
};
use reifydb_type::Result;

use crate::{ColumnStatistics, Partition, PartitionKey, backend::ColumnBackend, statistics::merge};

#[derive(Clone)]
pub struct MemoryColumnBackend {
	partitions: Arc<SkipMap<PartitionKey, Partition>>,
}

impl MemoryColumnBackend {
	pub fn new() -> Self {
		Self {
			partitions: Arc::new(SkipMap::new()),
		}
	}

	pub fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()> {
		let key = PartitionKey::new(0, version);
		let partition = Partition::new(key.clone(), columns);

		self.partitions.insert(key, partition);
		Ok(())
	}

	pub fn scan(&self, version: CommitVersion, _column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		let key = PartitionKey::new(0, version);

		if let Some(_entry) = self.partitions.get(&key) {
			todo!("decompress columns")
		} else {
			// No data for this version
			Ok(vec![])
		}
	}

	pub fn statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		let stats: Vec<_> = self
			.partitions
			.iter()
			.filter_map(|entry| entry.value().statistics.get(column_index).cloned())
			.collect();

		merge(&stats)
	}

	pub fn partition_count(&self) -> usize {
		self.partitions.len()
	}

	pub fn compressed_size(&self) -> usize {
		self.partitions.iter().map(|entry| entry.value().compressed_size).sum()
	}

	pub fn uncompressed_size(&self) -> usize {
		self.partitions.iter().map(|entry| entry.value().uncompressed_size).sum()
	}

	pub fn name(&self) -> &str {
		"memory"
	}

	pub fn is_available(&self) -> bool {
		true
	}
}

impl Default for MemoryColumnBackend {
	fn default() -> Self {
		Self::new()
	}
}

impl ColumnBackend for MemoryColumnBackend {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()> {
		self.insert(version, columns)
	}

	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		self.scan(version, column_indices)
	}

	fn statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		self.statistics(column_index)
	}

	fn partition_count(&self) -> usize {
		self.partition_count()
	}

	fn compressed_size(&self) -> usize {
		self.compressed_size()
	}

	fn uncompressed_size(&self) -> usize {
		self.uncompressed_size()
	}

	fn name(&self) -> &str {
		self.name()
	}

	fn is_available(&self) -> bool {
		self.is_available()
	}
}
