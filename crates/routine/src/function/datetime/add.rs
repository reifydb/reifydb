// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::datetime;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::{datetimes, durations},
	datetime::DateTime,
	value_type::ValueType,
};

pub struct DateTimeAdd {
	info: RoutineInfo,
}

impl Default for DateTimeAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeAdd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::add"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeAdd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::DateTime
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let dt_data = ColumnView::try_from(&args[0])?;
		let dur_data = ColumnView::try_from(&args[1])?;
		let row_count = dt_data.len();

		let result_data = match (&dt_data.data, &dur_data.data) {
			(ViewData::DateTime(dt_container), ViewData::Duration(dur_container)) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (datetimes(dt_container).get(i), durations(dur_container).get(i)) {
						(Some(dt), Some(dur)) => match dt.add_duration(dur) {
							Ok(result) => container.push(result),
							Err(err) => {
								return Err(RoutineError::FunctionExecutionFailed {
									function: ctx.fragment.clone(),
									reason: format!("{}", err),
								});
							}
						},
						_ => container.push(DateTime::default()),
					}
				}

				datetime(ctx.fragment.text(), container)
			}
			(ViewData::DateTime(_), _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::Duration],
					actual: dur_data.get_type(),
				});
			}
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: dt_data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateTimeAdd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
