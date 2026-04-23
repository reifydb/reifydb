// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextPadLeft {
	info: FunctionInfo,
}

impl Default for TextPadLeft {
	fn default() -> Self {
		Self::new()
	}
}

impl TextPadLeft {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::pad_left"),
		}
	}
}

impl Function for TextPadLeft {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 3 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let str_col = &args[0];
		let len_col = &args[1];
		let pad_col = &args[2];

		let (str_data, str_bv) = str_col.data().unwrap_option();
		let (len_data, len_bv) = len_col.data().unwrap_option();
		let (pad_data, pad_bv) = pad_col.data().unwrap_option();
		let row_count = str_data.len();

		let pad_container = match pad_data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => container,
			other => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 2,
					expected: vec![Type::Utf8],
					actual: other.get_type(),
				});
			}
		};

		match str_data {
			ColumnBuffer::Utf8 {
				container: str_container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if !str_container.is_defined(i) || !pad_container.is_defined(i) {
						result_data.push(String::new());
						continue;
					}

					let target_len = match len_data {
						ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int8(c) => c.get(i).copied(),
						ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i64),
						_ => {
							return Err(FunctionError::InvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									Type::Int1,
									Type::Int2,
									Type::Int4,
									Type::Int8,
								],
								actual: len_data.get_type(),
							});
						}
					};

					match target_len {
						Some(n) if n >= 0 => {
							let s = &str_container[i];
							let pad_char = &pad_container[i];
							let char_count = s.chars().count();
							let target = n as usize;

							if char_count >= target {
								result_data.push(s.to_string());
							} else {
								let pad_chars: Vec<char> = pad_char.chars().collect();
								if pad_chars.is_empty() {
									result_data.push(s.to_string());
								} else {
									let needed = target - char_count;
									let mut padded = String::with_capacity(
										s.len() + needed
											* pad_chars[0].len_utf8(),
									);
									for j in 0..needed {
										padded.push(
											pad_chars[j % pad_chars.len()]
										);
									}
									padded.push_str(s);
									result_data.push(padded);
								}
							}
						}
						Some(_) => {
							result_data.push(String::new());
						}
						None => {
							result_data.push(String::new());
						}
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				};

				// Combine all three bitvecs
				let mut combined_bv: Option<BitVec> = None;
				for bv in [str_bv, len_bv, pad_bv].into_iter().flatten() {
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
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
