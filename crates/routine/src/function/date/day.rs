// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_arith::temporal::{DatePart, date_part};
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4_with_bitvec;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::dates,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct DateDay {
	info: RoutineInfo,
}

impl Default for DateDay {
	fn default() -> Self {
		Self::new()
	}
}

impl DateDay {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::day"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateDay {
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
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Date(container) => {
				let parts = date_part(*container, DatePart::Day).map_err(|err| {
					RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: err.to_string(),
					}
				})?;
				let parts = parts.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
					RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: "date part of a date column is not a 32 bit integer"
							.to_string(),
					}
				})?;

				if parts.null_count() == 0 {
					array_column(
						ctx.fragment.text(),
						ValueType::Int4,
						Arc::new(Int32Array::new(parts.values().clone(), None)),
					)
				} else {
					let values = dates(container);
					let mut result = Vec::with_capacity(row_count);
					let mut res_bitvec = Vec::with_capacity(row_count);

					for i in 0..row_count {
						if parts.is_valid(i) {
							result.push(parts.value(i));
							res_bitvec.push(true);
						} else if let Some(date) = values.get(i) {
							result.push(date.day() as i32);
							res_bitvec.push(true);
						} else {
							result.push(0);
							res_bitvec.push(false);
						}
					}

					int4_with_bitvec(ctx.fragment.text(), result, res_bitvec)
				}
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

impl Function for DateDay {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
