// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, types::Int32Type};
use arrow_schema::FieldRef;
use arrow_string::length::length;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

use crate::function::support::column::array_column;

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;

		match &data.data {
			ViewData::Utf8 {
				container,
				..
			} => {
				let byte_lengths =
					length(*container).map_err(|err| RoutineError::FunctionExecutionFailed {
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

				let result_data = Arc::new(byte_lengths.unary::<_, Int32Type>(|len| len as i32));
				Ok(array_column(ctx.fragment.text(), ValueType::Int4, result_data))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: data.get_type(),
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
