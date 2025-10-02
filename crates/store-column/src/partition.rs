// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, value::column::CompressedColumn};

use crate::ColumnStatistics;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
	pub source: u64,
	pub version: CommitVersion,
}

impl PartitionKey {
	pub fn new(source: u64, version: CommitVersion) -> Self {
		Self {
			source,
			version,
		}
	}
}

#[derive(Clone)]
pub struct Partition {
	pub key: PartitionKey,
	pub columns: Vec<CompressedColumn>,
	pub statistics: Vec<ColumnStatistics>,
	pub row_count: usize,
	pub compressed_size: usize,
	pub uncompressed_size: usize,
}

impl Partition {
	pub fn new(key: PartitionKey, columns: Vec<CompressedColumn>) -> Self {
		let row_count = columns.first().map_or(0, |c| c.row_count);
		let compressed_size: usize = columns.iter().map(|c| c.data.len()).sum();
		let uncompressed_size: usize = columns.iter().map(|c| c.uncompressed_size).sum();

		// let statistics = columns.iter().map(|col| ColumnStatistics::from_compressed(col)).collect();

		Self {
			key,
			columns,
			statistics: vec![],
			row_count,
			compressed_size,
			uncompressed_size,
		}
	}

	pub fn compression_ratio(&self) -> f64 {
		if self.uncompressed_size == 0 {
			1.0
		} else {
			self.compressed_size as f64 / self.uncompressed_size as f64
		}
	}
}
