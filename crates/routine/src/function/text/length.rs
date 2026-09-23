// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, Int64Array, types::Int32Type};
use arrow_string::length::length;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct TextLength {
	info: RoutineInfo,
}

impl Default for TextLength {
	fn default() -> Self {
		Self::new()
	}
}

impl TextLength {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::length"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextLength {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let byte_lengths =
					length(container).map_err(|err| RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: err.to_string(),
					})?;
				let byte_lengths =
					byte_lengths.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
						RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: "byte length of a text column is not a 64 bit integer"
								.to_string(),
						}
					})?;

				let result_data =
					ColumnBuffer::Int4(byte_lengths.unary::<_, Int32Type>(|len| len as i32));
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

impl Function for TextLength {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
