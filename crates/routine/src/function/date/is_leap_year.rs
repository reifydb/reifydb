// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::bool_with_bitvec;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::dates,
	date::Date,
	value_type::ValueType,
};

pub struct DateIsLeapYear {
	info: RoutineInfo,
}

impl Default for DateIsLeapYear {
	fn default() -> Self {
		Self::new()
	}
}

impl DateIsLeapYear {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::is_leap_year"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateIsLeapYear {
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
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Date(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = dates(container).get(i) {
						result.push(Date::is_leap_year(date.year()));
						res_bitvec.push(true);
					} else {
						result.push(false);
						res_bitvec.push(false);
					}
				}

				bool_with_bitvec(ctx.fragment.text(), result, res_bitvec)
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

impl Function for DateIsLeapYear {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
