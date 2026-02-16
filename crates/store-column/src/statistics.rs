// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::ColumnStatistics;

pub fn merge(stats: &[ColumnStatistics]) -> Option<ColumnStatistics> {
	if stats.is_empty() {
		return None;
	}

	let total_none_count: usize = stats.iter().map(|s| s.none_count).sum();
	let total_compressed: usize = stats.iter().map(|s| s.compressed_size).sum();
	let total_uncompressed: usize = stats.iter().map(|s| s.uncompressed_size).sum();

	Some(ColumnStatistics {
		min_value: None, // TODO: Find global min
		max_value: None, // TODO: Find global max
		none_count: total_none_count,
		distinct_count: None,
		compression_type: stats[0].compression_type.clone(),
		compression_ratio: if total_uncompressed > 0 {
			total_compressed as f64 / total_uncompressed as f64
		} else {
			1.0
		},
		compressed_size: total_compressed,
		uncompressed_size: total_uncompressed,
	})
}
