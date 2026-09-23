// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_arith::temporal::{DatePart, date_part};
use arrow_array::{Array, Int32Array};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::dates, value_type::ValueType};

pub struct DateMonth {
	info: RoutineInfo,
}

impl Default for DateMonth {
	fn default() -> Self {
		Self::new()
	}
}

impl DateMonth {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::month"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateMonth {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Date(container) => {
				let parts = date_part(container, DatePart::Month).map_err(|err| {
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
					ColumnBuffer::Int4(Int32Array::new(parts.values().clone(), None))
				} else {
					let values = dates(container);
					let mut result = Vec::with_capacity(row_count);
					let mut res_bitvec = Vec::with_capacity(row_count);

					for i in 0..row_count {
						if parts.is_valid(i) {
							result.push(parts.value(i));
							res_bitvec.push(true);
						} else if let Some(date) = values.get(i) {
							result.push(date.month() as i32);
							res_bitvec.push(true);
						} else {
							result.push(0);
							res_bitvec.push(false);
						}
					}

					ColumnBuffer::int4_with_bitvec(result, res_bitvec)
				}
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::Date],
					actual: other.get_type(),
				});
			}
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for DateMonth {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
