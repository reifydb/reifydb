// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct TextAscii {
	info: RoutineInfo,
}

impl Default for TextAscii {
	fn default() -> Self {
		Self::new()
	}
}

impl TextAscii {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::ascii"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextAscii {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					let s = container.value(i);
					let code_point = s.chars().next().map(|c| c as i32).unwrap_or(0);
					result_data.push(code_point);
				}

				let result_data = ColumnBuffer::int4(result_data);
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
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

impl Function for TextAscii {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
