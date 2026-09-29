// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use arrow_string::like::ends_with;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

use crate::function::support::column::array_column;

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let str_data = ColumnView::try_from(&args[0])?;
		let suffix_data = ColumnView::try_from(&args[1])?;

		match (&str_data.data, &suffix_data.data) {
			(
				ViewData::Utf8 {
					container: str_container,
					..
				},
				ViewData::Utf8 {
					container: suffix_container,
					..
				},
			) => {
				let result_col_data = ends_with(*str_container, *suffix_container).map_err(|err| {
					RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: err.to_string(),
					}
				})?;

				Ok(array_column(ctx.fragment.text(), ValueType::Boolean, Arc::new(result_col_data)))
			}
			(
				ViewData::Utf8 {
					..
				},
				_,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: suffix_data.get_type(),
			}),
			(_, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: str_data.get_type(),
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
