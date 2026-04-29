// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{container::utf8::Utf8Container, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TextSubstring {
	info: RoutineInfo,
}

impl Default for TextSubstring {
	fn default() -> Self {
		Self::new()
	}
}

impl TextSubstring {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::substring"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextSubstring {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		// Validate exactly 3 arguments
		if args.len() != 3 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let text_col = &args[0];
		let start_col = &args[1];
		let length_col = &args[2];

		let (text_data, text_bv) = text_col.unwrap_option();
		let (start_data, start_bv) = start_col.unwrap_option();
		let (length_data, length_bv) = length_col.unwrap_option();
		let row_count = text_data.len();

		match (text_data, start_data, length_data) {
			(
				ColumnBuffer::Utf8 {
					container: text_container,
					max_bytes,
				},
				ColumnBuffer::Int4(start_container),
				ColumnBuffer::Int4(length_container),
			) => {
				let mut result_data = Vec::with_capacity(text_container.len());

				for i in 0..row_count {
					if text_container.is_defined(i)
						&& start_container.is_defined(i) && length_container.is_defined(i)
					{
						let original_str = text_container.get(i).unwrap();
						let start_pos = start_container.get(i).copied().unwrap_or(0);
						let length = length_container.get(i).copied().unwrap_or(0);

						// Get the substring with proper Unicode handling
						let chars: Vec<char> = original_str.chars().collect();
						let chars_len = chars.len();

						// Convert negative start to positive index from end
						let start_idx = if start_pos < 0 {
							chars_len.saturating_sub((-start_pos) as usize)
						} else {
							start_pos as usize
						};
						let length_usize = if length < 0 {
							0
						} else {
							length as usize
						};

						let substring = if start_idx >= chars_len {
							// Start position is beyond string length
							String::new()
						} else {
							let end_idx = (start_idx + length_usize).min(chars_len);
							chars[start_idx..end_idx].iter().collect()
						};

						result_data.push(substring);
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: *max_bytes,
				};

				// Combine all three bitvecs
				let mut combined_bv: Option<BitVec> = None;
				for bv in [text_bv, start_bv, length_bv].into_iter().flatten() {
					combined_bv = Some(match combined_bv {
						Some(existing) => existing.and(bv),
						None => bv.clone(),
					});
				}

				let final_data = match combined_bv {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv,
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			// Handle cases where start/length are different integer types
			(
				ColumnBuffer::Utf8 {
					container: text_container,
					max_bytes,
				},
				start_d,
				length_d,
			) => {
				let mut result_data = Vec::with_capacity(text_container.len());

				for i in 0..row_count {
					if text_container.is_defined(i) {
						let original_str = text_container.get(i).unwrap();

						// Extract start position from various integer types
						let start_pos = match start_d {
							ColumnBuffer::Int1(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnBuffer::Int2(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnBuffer::Int4(container) => {
								container.get(i).copied().unwrap_or(0)
							}
							ColumnBuffer::Int8(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							_ => 0,
						};

						// Extract length from various integer types
						let length = match length_d {
							ColumnBuffer::Int1(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnBuffer::Int2(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							ColumnBuffer::Int4(container) => {
								container.get(i).copied().unwrap_or(0)
							}
							ColumnBuffer::Int8(container) => {
								container.get(i).map(|&v| v as i32).unwrap_or(0)
							}
							_ => 0,
						};

						// Get the substring with proper Unicode handling
						let chars: Vec<char> = original_str.chars().collect();
						let chars_len = chars.len();

						// Convert negative start to positive index from end
						let start_idx = if start_pos < 0 {
							chars_len.saturating_sub((-start_pos) as usize)
						} else {
							start_pos as usize
						};
						let length_usize = if length < 0 {
							0
						} else {
							length as usize
						};

						let substring = if start_idx >= chars_len {
							// Start position is beyond string length
							String::new()
						} else {
							let end_idx = (start_idx + length_usize).min(chars_len);
							chars[start_idx..end_idx].iter().collect()
						};

						result_data.push(substring);
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: *max_bytes,
				};

				// Combine all three bitvecs
				let mut combined_bv: Option<BitVec> = None;
				for bv in [text_bv, start_bv, length_bv].into_iter().flatten() {
					combined_bv = Some(match combined_bv {
						Some(existing) => existing.and(bv),
						None => bv.clone(),
					});
				}

				let final_data = match combined_bv {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv,
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			(other, _, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextSubstring {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
