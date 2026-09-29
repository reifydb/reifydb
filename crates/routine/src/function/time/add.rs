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
	container::temporal_array::{durations, time_array, times},
	time::Time,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct TimeAdd {
	info: RoutineInfo,
}

impl Default for TimeAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeAdd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::add"),
		}
	}
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl<'a> Routine<FunctionContext<'a>> for TimeAdd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let time_data = ColumnView::try_from(&args[0])?;
		let dur_data = ColumnView::try_from(&args[1])?;

		match (&time_data.data, &dur_data.data) {
			(ViewData::Time(time_container), ViewData::Duration(dur_container)) => {
				let row_count = time_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (times(time_container).get(i), durations(dur_container).get(i)) {
						(Some(time), Some(dur)) => {
							let time_nanos = time.to_nanos_since_midnight() as i64;
							let dur_nanos =
								dur.get_nanos() + dur.get_days() as i64 * NANOS_PER_DAY;

							let result_nanos =
								(time_nanos + dur_nanos).rem_euclid(NANOS_PER_DAY);
							match Time::from_nanos_since_midnight(result_nanos as u64) {
								Some(result) => container.push(result),
								None => container.push(Time::default()),
							}
						}
						_ => container.push(Time::default()),
					}
				}

				Ok(array_column(ctx.fragment.text(), ValueType::Time, Arc::new(time_array(container))))
			}
			(ViewData::Time(_), _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Duration],
				actual: dur_data.get_type(),
			}),
			(_, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Time],
				actual: time_data.get_type(),
			}),
		}
	}
}

impl Function for TimeAdd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
