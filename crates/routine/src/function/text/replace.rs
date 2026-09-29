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

pub struct TextReplace {
	info: RoutineInfo,
}

impl Default for TextReplace {
	fn default() -> Self {
		Self::new()
	}
}

impl TextReplace {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::replace"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextReplace {
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
		let from_data = ColumnView::try_from(&args[1])?;
		let to_data = ColumnView::try_from(&args[2])?;

		let row_count = str_data.len();

		match (&str_data.data, &from_data.data, &to_data.data) {
			(
				ViewData::Utf8 {
					container: str_container,
					..
				},
				ViewData::Utf8 {
					container: from_container,
					..
				},
				ViewData::Utf8 {
					container: to_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if i < str_container.len() && i < from_container.len() && i < to_container.len()
					{
						let s = str_container.value(i);
						let from = from_container.value(i);
						let to = to_container.value(i);
						result_data.push(s.replace(from, to));
					} else {
						result_data.push(String::new());
					}
				}

				Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, LargeStringArray::from(result_data)))
			}
			(
				ViewData::Utf8 {
					..
				},
				ViewData::Utf8 {
					..
				},
				_,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![ValueType::Utf8],
				actual: to_data.get_type(),
			}),
			(
				ViewData::Utf8 {
					..
				},
				_,
				_,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: from_data.get_type(),
			}),
			(_, _, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: str_data.get_type(),
			}),
		}
	}
}

impl Function for TextReplace {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
