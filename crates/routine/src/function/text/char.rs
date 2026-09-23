// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::LargeStringArray;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{constraint::bytes::MaxBytes, value_type::ValueType};

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub struct TextChar {
	info: RoutineInfo,
}

impl Default for TextChar {
	fn default() -> Self {
		Self::new()
	}
}

impl TextChar {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::char"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextChar {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Int1(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Int2(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Int4(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Int8(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Uint1(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Uint2(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			ColumnBuffer::Uint4(c) => {
				convert_to_char(ctx, row_count, |i| c.values().get(i).map(|&v| v as i128))?
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![
						ValueType::Int1,
						ValueType::Int2,
						ValueType::Int4,
						ValueType::Int8,
						ValueType::Uint1,
						ValueType::Uint2,
						ValueType::Uint4,
					],
					actual: other.get_type(),
				});
			}
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for TextChar {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}

fn convert_to_char<F>(
	ctx: &FunctionContext,
	row_count: usize,
	get_value: F,
) -> Result<ColumnBuffer, RoutineError>
where
	F: Fn(usize) -> Option<i128>,
{
	let mut result_data = Vec::with_capacity(row_count);

	for i in 0..row_count {
		match get_value(i) {
			Some(code_point) => {
				let ch = u32::try_from(code_point)
					.ok()
					.and_then(char::from_u32)
					.ok_or_else(|| {
						failed(ctx, format!("{code_point} is not a character code point"))
					})?;
				result_data.push(ch.to_string());
			}
			None => {
				result_data.push(String::new());
			}
		}
	}

	Ok(ColumnBuffer::Utf8 {
		container: LargeStringArray::from(result_data),
		max_bytes: MaxBytes::MAX,
	})
}
