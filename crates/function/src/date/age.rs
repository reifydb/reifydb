// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, duration::Duration, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateAge;

impl DateAge {
	pub fn new() -> Self {
		Self
	}
}

/// Compute calendar-aware age between two dates.
/// Returns Duration with months + days components.
pub fn date_age(d1: &Date, d2: &Date) -> Duration {
	let y1 = d1.year();
	let m1 = d1.month() as i32;
	let day1 = d1.day() as i32;

	let y2 = d2.year();
	let m2 = d2.month() as i32;
	let day2 = d2.day() as i32;

	let mut years = y1 - y2;
	let mut months = m1 - m2;
	let mut days = day1 - day2;

	if days < 0 {
		months -= 1;
		// Borrow days from previous month (relative to d2's perspective)
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
		days += Date::days_in_month(borrow_year, borrow_month as u32) as i32;
	}

	if months < 0 {
		years -= 1;
		months += 12;
	}

	let total_months = years * 12 + months;
	Duration::new(total_months, days, 0)
}

impl ScalarFunction for DateAge {
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
			(ColumnData::Date(container1), ColumnData::Date(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(d1), Some(d2)) => {
							container.push(date_age(&d1, &d2));
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::Date(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Date],
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
}
