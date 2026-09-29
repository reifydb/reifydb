// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::bytes::MaxBytes,
	value_type::ValueType,
};

use crate::function::support::column::utf8_column;

pub struct TextPadRight {
	info: RoutineInfo,
}

impl Default for TextPadRight {
	fn default() -> Self {
		Self::new()
	}
}

impl TextPadRight {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::pad_right"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextPadRight {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let str_data = ColumnView::try_from(&args[0])?;
		let len_data = ColumnView::try_from(&args[1])?;
		let pad_data = ColumnView::try_from(&args[2])?;

		let row_count = str_data.len();

		let pad_container = match &pad_data.data {
			ViewData::Utf8 {
				container,
				..
			} => container,
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 2,
					expected: vec![ValueType::Utf8],
					actual: pad_data.get_type(),
				});
			}
		};

		match &str_data.data {
			ViewData::Utf8 {
				container: str_container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if i >= str_container.len() || i >= pad_container.len() {
						result_data.push(String::new());
						continue;
					}

					let target_len = match &len_data.data {
						ViewData::Int1(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int2(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int4(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int8(c) => c.values().get(i).copied(),
						ViewData::Uint1(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Uint2(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Uint4(c) => c.values().get(i).map(|&v| v as i64),
						_ => {
							return Err(RoutineError::FunctionInvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									ValueType::Int1,
									ValueType::Int2,
									ValueType::Int4,
									ValueType::Int8,
								],
								actual: len_data.get_type(),
							});
						}
					};

					match target_len {
						Some(n) if n >= 0 => {
							let s = str_container.value(i);
							let pad_char = pad_container.value(i);
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
									padded.push_str(s);
									for j in 0..needed {
										padded.push(
											pad_chars[j % pad_chars.len()]
										);
									}
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

				Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, LargeStringArray::from(result_data)))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: str_data.get_type(),
			}),
		}
	}
}

impl Function for TextPadRight {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
