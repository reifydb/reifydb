// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_string::concat_elements::concat_elements_utf8_many;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{constraint::bytes::MaxBytes, value_type::ValueType};

pub struct TextConcat {
	info: RoutineInfo,
}

impl Default for TextConcat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextConcat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::concat"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextConcat {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let mut containers = Vec::with_capacity(args.len());

		for (idx, col) in args.iter().enumerate() {
			match col.data() {
				ColumnBuffer::Utf8 {
					container,
					..
				} => containers.push(container),
				other => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: idx,
						expected: vec![ValueType::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let container = concat_elements_utf8_many(&containers).map_err(|err| {
			RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: err.to_string(),
			}
		})?;

		let result_col_data = ColumnBuffer::Utf8 {
			container,
			max_bytes: MaxBytes::MAX,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_col_data)]))
	}
}

impl Function for TextConcat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::AtLeast(2)
	}
}
