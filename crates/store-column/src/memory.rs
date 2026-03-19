// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	common::CommitVersion,
	value::column::{compressed::CompressedColumn, data::ColumnData},
};
use reifydb_type::Result;

use crate::{ColumnStatistics, ColumnStore, Partition, PartitionKey, statistics::merge};

#[derive(Clone)]
pub struct MemoryColumnStore {
	partitions: Arc<SkipMap<PartitionKey, Partition>>,
}

impl MemoryColumnStore {
	pub fn new() -> Self {
		Self {
			partitions: Arc::new(SkipMap::new()),
		}
	}
}

impl Default for MemoryColumnStore {
	fn default() -> Self {
		Self::new()
	}
}

impl ColumnStore for MemoryColumnStore {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()> {
		let key = PartitionKey::new(0, version);
		let partition = Partition::new(key.clone(), columns);

		self.partitions.insert(key, partition);
		Ok(())
	}

	fn scan(&self, version: CommitVersion, _column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		let key = PartitionKey::new(0, version);

		if let Some(_entry) = self.partitions.get(&key) {
			todo!("decompress columns")
		} else {
			// No data for this version
			Ok(vec![])
		}
	}

	fn statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		let stats: Vec<_> = self
			.partitions
			.iter()
			.filter_map(|entry| entry.value().statistics.get(column_index).cloned())
			.collect();

		merge(&stats)
	}

	fn partition_count(&self) -> usize {
		self.partitions.len()
	}

	fn compressed_size(&self) -> usize {
		self.partitions.iter().map(|entry| entry.value().compressed_size).sum()
	}

	fn uncompressed_size(&self) -> usize {
		self.partitions.iter().map(|entry| entry.value().uncompressed_size).sum()
	}
}
