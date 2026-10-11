// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

use crate::function::support::{coerce::read_i32, column::utf8_column};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let text_data = ColumnView::try_from(&args[0])?;
		let start_data = ColumnView::try_from(&args[1])?;
		let length_data = ColumnView::try_from(&args[2])?;

		let row_count = text_data.len();

		match (&text_data.data, &start_data.data, &length_data.data) {
			(
				ViewData::Utf8 {
					container: text_container,
					max_bytes,
				},
				ViewData::Int4(start_container),
				ViewData::Int4(length_container),
			) => {
				let mut result_data = Vec::with_capacity(text_container.len());

				for i in 0..row_count {
					if i < text_container.len()
						&& i < start_container.len()
						&& i < length_container.len()
					{
						let original_str = text_container.value(i);
						let start_pos = start_container.values().get(i).copied().unwrap_or(0);
						let length = length_container.values().get(i).copied().unwrap_or(0);

						let chars: Vec<char> = original_str.chars().collect();
						let chars_len = chars.len();

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

				Ok(utf8_column(ctx.fragment.text(), *max_bytes, LargeStringArray::from(result_data)))
			}

			(
				ViewData::Utf8 {
					container: text_container,
					max_bytes,
				},
				_,
				_,
			) => {
				let mut result_data = Vec::with_capacity(text_container.len());

				for i in 0..row_count {
					if i < text_container.len() {
						let original_str = text_container.value(i);

						let start_pos = read_i32(&ctx.fragment, &start_data, i)?.unwrap_or(0);

						let length = read_i32(&ctx.fragment, &length_data, i)?.unwrap_or(0);

						let chars: Vec<char> = original_str.chars().collect();
						let chars_len = chars.len();

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

				Ok(utf8_column(ctx.fragment.text(), *max_bytes, LargeStringArray::from(result_data)))
			}
			(_, _, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: text_data.get_type(),
			}),
		}
	}
}

impl Function for TextSubstring {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
