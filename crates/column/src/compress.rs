// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[derive(Clone, Debug)]
pub struct CompressConfig {
	pub sample_size: usize,
	pub sample_count: usize,
	pub max_depth: u8,
	pub min_compression_ratio: f32,
}

impl Default for CompressConfig {
	fn default() -> Self {
		Self {
			sample_size: 1024,
			sample_count: 4,
			max_depth: 3,
			min_compression_ratio: 0.85,
		}
	}
}
