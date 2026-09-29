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
	container::temporal_array::{dates, times},
	datetime::DateTime,
	value_type::ValueType,
};

pub struct DateTimeNew {
	info: RoutineInfo,
}

impl Default for DateTimeNew {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeNew {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::new"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeNew {
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
		let date_data = ColumnView::try_from(&args[0])?;
		let time_data = ColumnView::try_from(&args[1])?;
		let row_count = date_data.len();

		let result_data =
			match (&date_data.data, &time_data.data) {
				(ViewData::Date(date_container), ViewData::Time(time_container)) => {
					let mut container = Vec::with_capacity(row_count);

					for i in 0..row_count {
						match (dates(date_container).get(i), times(time_container).get(i)) {
							(Some(date), Some(time)) => {
								match DateTime::new(
									date.year(),
									date.month(),
									date.day(),
									time.hour(),
									time.minute(),
									time.second(),
									time.nanosecond(),
								) {
									Some(dt) => container.push(dt),
									None => {
										return Err(RoutineError::FunctionExecutionFailed {
										function: ctx.fragment.clone(),
										reason: "datetime out of range".to_string(),
									});
									}
								}
							}
							_ => container.push(DateTime::default()),
						}
					}

					datetime(ctx.fragment.text(), container)
				}
				(ViewData::Date(_), _) => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![ValueType::Time],
						actual: time_data.get_type(),
					});
				}
				(_, _) => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 0,
						expected: vec![ValueType::Date],
						actual: date_data.get_type(),
					});
				}
			};

		Ok(result_data)
	}
}

impl Function for DateTimeNew {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
