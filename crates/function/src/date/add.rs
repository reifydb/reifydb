// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateAdd;

impl DateAdd {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateAdd {
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

		let date_col = columns.get(0).unwrap();
		let dur_col = columns.get(1).unwrap();

		match (date_col.data(), dur_col.data()) {
			(ColumnData::Date(date_container), ColumnData::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), dur_container.get(i)) {
						(Some(date), Some(dur)) => {
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

							// Convert to days_since_epoch and add days component
							if let Some(base) = Date::new(year, month as u32, day) {
								let total_days = base.to_days_since_epoch()
									+ dur.get_days() + (dur.get_nanos()
									/ 86_400_000_000_000)
									as i32;
								match Date::from_days_since_epoch(total_days) {
									Some(result) => container.push(result),
									None => container.push_default(),
								}
							} else {
								container.push_default();
							}
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::Date(container))
			}
			(ColumnData::Date(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
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
