// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::{date_array, dates},
	date::Date,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct DateStartOfYear {
	info: RoutineInfo,
}

impl Default for DateStartOfYear {
	fn default() -> Self {
		Self::new()
	}
}

impl DateStartOfYear {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::start_of_year"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateStartOfYear {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Date
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Date(container) => {
				let mut result = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = dates(container).get(i) {
						match Date::new(date.year(), 1, 1) {
							Some(d) => result.push(d),
							None => result.push(Date::default()),
						}
					} else {
						result.push(Date::default());
					}
				}

				array_column(ctx.fragment.text(), ValueType::Date, Arc::new(date_array(result)))
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::Date],
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateStartOfYear {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
