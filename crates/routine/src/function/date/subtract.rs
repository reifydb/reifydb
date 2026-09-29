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
	container::temporal_array::{date_array, dates, durations},
	date::Date,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct DateSubtract {
	info: RoutineInfo,
}

impl Default for DateSubtract {
	fn default() -> Self {
		Self::new()
	}
}

impl DateSubtract {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::subtract"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateSubtract {
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
		let date_data = ColumnView::try_from(&args[0])?;
		let dur_data = ColumnView::try_from(&args[1])?;
		let row_count = date_data.len();

		let result_data = match (&date_data.data, &dur_data.data) {
			(ViewData::Date(date_container), ViewData::Duration(dur_container)) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dates(date_container).get(i), durations(dur_container).get(i)) {
						(Some(date), Some(dur)) => {
							let mut year = date.year();
							let mut month = date.month() as i32;
							let mut day = date.day();

							let total_months = month - dur.get_months();
							year += (total_months - 1).div_euclid(12);
							month = (total_months - 1).rem_euclid(12) + 1;

							let max_day = days_in_month(year, month as u32);
							if day > max_day {
								day = max_day;
							}

							if let Some(base) = Date::new(year, month as u32, day) {
								let total_days = base.to_days_since_epoch()
									- dur.get_days()
									- (dur.get_nanos() / 86_400_000_000_000) as i32;
								match Date::from_days_since_epoch(total_days) {
									Some(result) => container.push(result),
									None => container.push(Date::default()),
								}
							} else {
								container.push(Date::default());
							}
						}
						_ => container.push(Date::default()),
					}
				}

				array_column(ctx.fragment.text(), ValueType::Date, Arc::new(date_array(container)))
			}
			(ViewData::Date(_), _) => {
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
					expected: vec![ValueType::Date],
					actual: date_data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateSubtract {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}

fn days_in_month(year: i32, month: u32) -> u32 {
	match month {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		2 => {
			if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
				29
			} else {
				28
			}
		}
		_ => 0,
	}
}
