// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, LargeStringArray};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct TextReverse {
	info: RoutineInfo,
}

impl Default for TextReverse {
	fn default() -> Self {
		Self::new()
	}
}

impl TextReverse {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::reverse"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextReverse {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if i < container.len() {
						let reversed: String = container.value(i).chars().rev().collect();
						result_data.push(reversed);
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: LargeStringArray::from(result_data),
					max_bytes: *max_bytes,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_col_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextReverse {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
