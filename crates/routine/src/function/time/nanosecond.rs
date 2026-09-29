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
	container::temporal_array::times,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct TimeNanosecond {
	info: RoutineInfo,
}

impl Default for TimeNanosecond {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeNanosecond {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::nanosecond"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeNanosecond {
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

		match &data.data {
			ViewData::Time(container) => {
				let parts = date_part(*container, DatePart::Nanosecond).map_err(|err| {
					RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: err.to_string(),
					}
				})?;
				let parts = parts.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
					RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: "time part of a time column is not a 32 bit integer"
							.to_string(),
					}
				})?;

				let result_data = if parts.null_count() == 0 {
					array_column(
						ctx.fragment.text(),
						ValueType::Int4,
						Arc::new(Int32Array::new(parts.values().clone(), None)),
					)
				} else {
					let values = times(container);
					let mut result = Vec::with_capacity(row_count);
					let mut res_bitvec = Vec::with_capacity(row_count);

					for i in 0..row_count {
						if parts.is_valid(i) {
							result.push(parts.value(i));
							res_bitvec.push(true);
						} else if let Some(time) = values.get(i) {
							result.push(time.nanosecond() as i32);
							res_bitvec.push(true);
						} else {
							result.push(0);
							res_bitvec.push(false);
						}
					}

					int4_with_bitvec(ctx.fragment.text(), result, res_bitvec)
				};
				Ok(result_data)
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Time],
				actual: data.get_type(),
			}),
		}
	}
}

impl Function for TimeNanosecond {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
