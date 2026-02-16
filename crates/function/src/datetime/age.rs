// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, duration::Duration, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTimeAge;

impl DateTimeAge {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeAge {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let col1 = columns.get(0).unwrap();
		let col2 = columns.get(1).unwrap();

		match (col1.data(), col2.data()) {
			(ColumnData::DateTime(container1), ColumnData::DateTime(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(dt1), Some(dt2)) => {
							// Extract time nanos since midnight
							let nanos1 = dt1.time().to_nanos_since_midnight() as i64;
							let nanos2 = dt2.time().to_nanos_since_midnight() as i64;
							let mut nanos_diff = nanos1 - nanos2;
							let mut days_borrow: i32 = 0;

							if nanos_diff < 0 {
								days_borrow = 1;
								nanos_diff += 86_400_000_000_000;
							}

							// Extract date parts
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
								) as i32;
							}

							if months < 0 {
								years -= 1;
								months += 12;
							}

							let total_months = years * 12 + months;
							container.push(Duration::new(total_months, days, nanos_diff));
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
		}
	}
}
