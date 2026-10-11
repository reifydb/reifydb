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
	date::Date,
	datetime::DateTime,
	value_type::ValueType,
};

pub struct DateTimeSubtract {
	info: RoutineInfo,
}

impl Default for DateTimeSubtract {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeSubtract {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::subtract"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeSubtract {
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
						(Some(dt), Some(dur)) => {
							let date = dt.date();
							let time = dt.time();
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

							if let Some(base_date) = Date::new(year, month as u32, day) {
								let base_days = base_date.to_days_since_epoch() as i64
									- dur.get_days() as i64;
								let time_nanos = time.to_nanos_since_midnight() as i64
									- dur.get_nanos();

								let total_nanos = base_days as i128
									* 86_400_000_000_000i128
									+ time_nanos as i128;

								match i64::try_from(total_nanos) {
									Ok(n) => {
										container.push(DateTime::from_nanos(n))
									}
									Err(_) => {
										return Err(RoutineError::FunctionExecutionFailed {
											function: ctx.fragment.clone(),
											reason: "datetime out of range".to_string(),
										});
									}
								}
							} else {
								return Err(RoutineError::FunctionExecutionFailed {
									function: ctx.fragment.clone(),
									reason: "datetime out of range".to_string(),
								});
							}
						}
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

impl Function for DateTimeSubtract {
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
