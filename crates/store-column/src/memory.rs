// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	CommitVersion,
	value::column::{ColumnData, Columns, CompressedColumn},
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

	/// Compress columns using the appropriate compressor for each column type
	pub fn compress_columns(&self, _columns: &Columns) -> Result<Vec<CompressedColumn>> {
		// let mut result = Vec::new();
		//
		// for column in columns.columns {
		// 	let compressor = select_compressor(column.data());
		// 	result.push(compressor.compress(column.data())?);
		// }
		//
		// Ok(result)
		todo!()
	}

	/// Decompress columns
	pub fn decompress_columns(&self, _compressed: &[CompressedColumn]) -> Result<Vec<ColumnData>> {
		// compressed
		// 	.iter()
		// 	.map(|col| {
		// 		// Select appropriate decompressor based on compression type
		// 		match col.compression {
		// 			reifydb_compression::CompressionType::Dictionary => {
		// 				let decompressor = DictionaryCompressor::new();
		// 				decompressor.decompress(col)
		// 			}
		// 			reifydb_compression::CompressionType::Delta => {
		// 				let decompressor = DeltaCompressor::new();
		// 				decompressor.decompress(col)
		// 			}
		// 			reifydb_compression::CompressionType::RunLength => {
		// 				let decompressor = RleCompressor::new();
		// 				decompressor.decompress(col)
		// 			}
		// 			reifydb_compression::CompressionType::BitPacking => {
		// 				let decompressor = BitPackCompressor::new();
		// 				decompressor.decompress(col)
		// 			}
		// 			reifydb_compression::CompressionType::Zstd => {
		// 				let decompressor = reifydb_compression::zstd::ZstdCompressor::new(3);
		// 				decompressor.decompress(col)
		// 			}
		// 			_ => {
		// 				unimplemented!()
		// 			}
		// 		}
		// 	})
		// 	.collect()
		todo!()
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

	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		let key = PartitionKey::new(0, version);

		if let Some(entry) = self.partitions.get(&key) {
			let partition = entry.value();

			let selected_columns: Vec<_> =
				column_indices.iter().filter_map(|&idx| partition.columns.get(idx)).cloned().collect();

			self.decompress_columns(&selected_columns)
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
