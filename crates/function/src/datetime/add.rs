// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, datetime::DateTime, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTimeAdd;

impl DateTimeAdd {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeAdd {
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

		let dt_col = columns.get(0).unwrap();
		let dur_col = columns.get(1).unwrap();

		match (dt_col.data(), dur_col.data()) {
			(ColumnData::DateTime(dt_container), ColumnData::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), dur_container.get(i)) {
						(Some(dt), Some(dur)) => {
							let date = dt.date();
							let time = dt.time();
							let mut year = date.year();
							let mut month = date.month() as i32;
							let mut day = date.day();

							// Add months component
							let total_months = month + dur.get_months();
							year += (total_months - 1).div_euclid(12);
							month = (total_months - 1).rem_euclid(12) + 1;

							// Clamp day to valid range for the new month
							let max_day = days_in_month(year, month as u32);
							if day > max_day {
								day = max_day;
							}

							// Convert to seconds since epoch and add day/nanos components
							if let Some(base_date) = Date::new(year, month as u32, day) {
								let base_days = base_date.to_days_since_epoch() as i64
									+ dur.get_days() as i64;
								let time_nanos = time.to_nanos_since_midnight() as i64
									+ dur.get_nanos();

								let total_seconds =
									base_days * 86400 + time_nanos / 1_000_000_000;
								let nano_part = (time_nanos % 1_000_000_000) as u32;

								match DateTime::from_parts(total_seconds, nano_part) {
									Ok(result) => container.push(result),
									Err(_) => container.push_undefined(),
								}
							} else {
								container.push_undefined();
							}
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::DateTime(container))
			}
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
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
