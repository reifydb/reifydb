// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{ScalarFunction, ScalarFunctionContext},
	value::{column::ColumnData, container::Utf8Container},
};

pub struct TextSubstring;

impl TextSubstring {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextSubstring {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() < 3 {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let text_column = columns.get(0).unwrap();
		let start_column = columns.get(1).unwrap();
		let length_column = columns.get(2).unwrap();

		match (text_column.data(), start_column.data(), length_column.data()) {
			(
				ColumnData::Utf8 {
					container: text_container,
					max_bytes,
				},
				ColumnData::Int4(start_container),
				ColumnData::Int4(length_container),
			) => {
				let mut result_data = Vec::with_capacity(text_container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if text_container.is_defined(i)
						&& start_container.is_defined(i) && length_container.is_defined(i)
					{
						let original_str = &text_container[i];
						let start_pos = start_container.get(i).copied().unwrap_or(0);
						let length = length_container.get(i).copied().unwrap_or(0);

						// Convert to 0-based indexing (RQL uses 0-based)
						let start_idx = if start_pos < 0 {
							0
						} else {
							start_pos as usize
						};
						let length_usize = if length < 0 {
							0
						} else {
							length as usize
						};

						let substring = if start_idx >= original_str.len() {
							// Start position is beyond string length
							String::new()
						} else {
							// Get the substring with proper Unicode handling
							let chars: Vec<char> = original_str.chars().collect();
							let end_idx = (start_idx + length_usize).min(chars.len());

							if start_idx < chars.len() {
								chars[start_idx..end_idx].iter().collect()
							} else {
								String::new()
							}
						};

						result_data.push(substring);
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: *max_bytes,
				})
			}
			// Handle cases where start/length are different integer types
			(
				ColumnData::Utf8 {
					container: text_container,
					max_bytes,
				},
				start_data,
				length_data,
			) => {
				let mut result_data = Vec::with_capacity(text_container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if text_container.is_defined(i) {
						let original_str = &text_container[i];

						// Extract start position from various integer types
						let start_pos = match start_data {
							ColumnData::Int1(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnData::Int2(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnData::Int4(container) => {
								container.get(i).copied().unwrap_or(0)
							}
							ColumnData::Int8(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							_ => 0,
						};

						// Extract length from various integer types
						let length = match length_data {
							ColumnData::Int1(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnData::Int2(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnData::Int4(container) => {
								container.get(i).copied().unwrap_or(0)
							}
							ColumnData::Int8(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							_ => 0,
						};

						// Convert to 0-based indexing
						let start_idx = if start_pos < 0 {
							0
						} else {
							start_pos as usize
						};
						let length_usize = if length < 0 {
							0
						} else {
							length as usize
						};

						let substring = if start_idx >= original_str.len() {
							// Start position is beyond string length
							String::new()
						} else {
							// Get the substring with proper Unicode handling
							let chars: Vec<char> = original_str.chars().collect();
							let end_idx = (start_idx + length_usize).min(chars.len());

							if start_idx < chars.len() {
								chars[start_idx..end_idx].iter().collect()
							} else {
								String::new()
							}
						};

						result_data.push(substring);
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: *max_bytes,
				})
			}
			_ => unimplemented!("TextSubstring requires text, start position, and length parameters"),
		}
	}
}
