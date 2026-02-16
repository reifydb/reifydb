// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct TextPadLeft;

impl TextPadLeft {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextPadLeft {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 3 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: columns.len(),
			});
		}

		let str_col = columns.get(0).unwrap();
		let len_col = columns.get(1).unwrap();
		let pad_col = columns.get(2).unwrap();

		let pad_data = match pad_col.data() {
			ColumnData::Utf8 {
				container,
				..
			} => container,
			other => {
				return Err(ScalarFunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 2,
					expected: vec![Type::Utf8],
					actual: other.get_type(),
				});
			}
		};

		match str_col.data() {
			ColumnData::Utf8 {
				container: str_container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if !str_container.is_defined(i) || !pad_data.is_defined(i) {
						result_data.push(String::new());
						continue;
					}

					let target_len = match len_col.data() {
						ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int8(c) => c.get(i).copied(),
						ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
						_ => {
							return Err(ScalarFunctionError::InvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									Type::Int1,
									Type::Int2,
									Type::Int4,
									Type::Int8,
								],
								actual: len_col.data().get_type(),
							});
						}
					};

					match target_len {
						Some(n) if n >= 0 => {
							let s = &str_container[i];
							let pad_char = &pad_data[i];
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

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				})
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
