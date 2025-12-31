// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[derive(Clone, Debug, PartialEq)]
pub enum CompressionType {
	None,
	Dictionary,
	Delta,
	RunLength,
	BitPacking,
}

#[derive(Clone, Debug)]
pub struct CompressedColumn {
	pub data: Vec<u8>,
	pub compression: CompressionType,
	pub uncompressed_size: usize,
	pub undefined_count: usize,
	pub row_count: usize,
}

impl CompressedColumn {
	pub fn compression_ratio(&self) -> f64 {
		if self.uncompressed_size == 0 {
			1.0
		} else {
			self.data.len() as f64 / self.uncompressed_size as f64
		}
	}
}
