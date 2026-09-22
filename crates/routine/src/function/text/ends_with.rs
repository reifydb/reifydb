// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Array;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct TextEndsWith {
	info: RoutineInfo,
}

impl Default for TextEndsWith {
	fn default() -> Self {
		Self::new()
	}
}

impl TextEndsWith {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::ends_with"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextEndsWith {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Boolean
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let str_data = &args[0];
		let suffix_data = &args[1];

		let row_count = str_data.len();

		match (str_data, suffix_data) {
			(
				ColumnBuffer::Utf8 {
					container: str_container,
					..
				},
				ColumnBuffer::Utf8 {
					container: suffix_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if i < str_container.len() && i < suffix_container.len() {
						let s = str_container.value(i);
						let suffix = suffix_container.value(i);
						result_data.push(s.ends_with(suffix));
						result_bitvec.push(true);
					} else {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}

				let result_col_data = ColumnBuffer::bool_with_bitvec(result_data, result_bitvec);

				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_col_data)]))
			}
			(
				ColumnBuffer::Utf8 {
					..
				},
				other,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextEndsWith {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
