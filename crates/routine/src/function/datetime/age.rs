// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::duration;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::datetimes,
	date::Date,
	duration::Duration,
	value_type::ValueType,
};

pub struct DateTimeAge {
	info: RoutineInfo,
}

impl Default for DateTimeAge {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeAge {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::age"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeAge {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data1 = ColumnView::try_from(&args[0])?;
		let data2 = ColumnView::try_from(&args[1])?;
		let row_count = data1.len();

		let result_data = match (&data1.data, &data2.data) {
			(ViewData::DateTime(container1), ViewData::DateTime(container2)) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (datetimes(container1).get(i), datetimes(container2).get(i)) {
						(Some(dt1), Some(dt2)) => {
							let nanos1 = dt1.time().to_nanos_since_midnight() as i64;
							let nanos2 = dt2.time().to_nanos_since_midnight() as i64;
							let mut nanos_diff = nanos1 - nanos2;
							let mut days_borrow: i32 = 0;

							if nanos_diff < 0 {
								days_borrow = 1;
								nanos_diff += 86_400_000_000_000;
							}

							let date1 = dt1.date();
							let date2 = dt2.date();

							let y1 = date1.year();
							let m1 = date1.month() as i32;
							let day1 = date1.day() as i32;

							let y2 = date2.year();
							let m2 = date2.month() as i32;
							let day2 = date2.day() as i32;

							let mut years = y1 - y2;
							let mut months = m1 - m2;
							let mut days = day1 - day2 - days_borrow;

							if days < 0 {
								months -= 1;
								let borrow_month = if m1 - 1 < 1 {
									12
								} else {
									m1 - 1
								};
								let borrow_year = if m1 - 1 < 1 {
									y1 - 1
								} else {
									y1
								};
								days += Date::days_in_month(
									borrow_year,
									borrow_month as u32,
								)
									as i32;
							}

							if months < 0 {
								years -= 1;
								months += 12;
							}

							let total_months = years * 12 + months;
							container.push(Duration::new(total_months, days, nanos_diff)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				duration(ctx.fragment.text(), container)
			}
			(ViewData::DateTime(_), _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::DateTime],
					actual: data2.get_type(),
				});
			}
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: data1.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateTimeAge {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
