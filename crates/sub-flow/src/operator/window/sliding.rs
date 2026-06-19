// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{WindowKind, WindowSize};

use super::operator::WindowOperator;

impl WindowOperator {
	pub fn get_sliding_window_ids(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Duration(duration),
				slide: WindowSize::Duration(slide_duration),
				..
			} => {
				let window_size_ms = duration.milliseconds().unwrap_or(0) as u64;
				let slide_ms = slide_duration.milliseconds().unwrap_or(0) as u64;
				let timestamp = timestamp_or_row_index;

				if slide_ms >= window_size_ms {
					vec![timestamp / slide_ms]
				} else {
					let min_window_id = if timestamp >= window_size_ms {
						(timestamp - window_size_ms + 1) / slide_ms
					} else {
						0
					};
					let max_window_id = timestamp / slide_ms;
					(min_window_id..=max_window_id)
						.filter(|&wid| {
							let start = wid * slide_ms;
							timestamp >= start && timestamp < start + window_size_ms
						})
						.collect()
				}
			}
			WindowKind::Sliding {
				size: WindowSize::Count(count),
				slide: WindowSize::Count(slide_count),
				..
			} => {
				let row_number = timestamp_or_row_index + 1;
				let min_window = if row_number > *count {
					(row_number - *count) / *slide_count
				} else {
					0
				};
				let max_window = (row_number - 1) / *slide_count;
				(min_window..=max_window)
					.filter(|&wid| {
						let start_row = wid * *slide_count + 1;
						let end_row = start_row + *count - 1;
						row_number >= start_row && row_number <= end_row
					})
					.collect()
			}
			_ => vec![0],
		}
	}
}
