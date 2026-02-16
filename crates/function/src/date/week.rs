// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateWeek;

impl DateWeek {
	pub fn new() -> Self {
		Self
	}
}

/// Compute the ISO 8601 week number for a date.
///
/// ISO 8601 rules:
/// - Weeks start on Monday
/// - Week 1 is the week containing January 4th
/// - A year has 52 or 53 weeks
/// - Jan 1-3 may belong to week 52/53 of the previous year
/// - Dec 29-31 may belong to week 1 of the next year
fn iso_week_number(date: &Date) -> i32 {
	let days = date.to_days_since_epoch();

	// ISO day of week: Mon=1..Sun=7
	let dow = ((days % 7 + 3) % 7 + 7) % 7 + 1;

	// Find the Thursday of this date's week (ISO weeks are identified by their Thursday)
	let thursday = days + (4 - dow);

	// Find Jan 1 of the year containing that Thursday
	let thursday_ymd = {
		let d = Date::from_days_since_epoch(thursday).unwrap();
		d.year()
	};
	let jan1 = Date::new(thursday_ymd, 1, 1).unwrap();
	let jan1_days = jan1.to_days_since_epoch();

	// Week number = how many weeks between Jan 1 of that year and the Thursday
	let week = (thursday - jan1_days) / 7 + 1;

	week
}

impl ScalarFunction for DateWeek {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let col = columns.get(0).unwrap();

		match col.data() {
			ColumnData::Date(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = container.get(i) {
						data.push(iso_week_number(&date));
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int4_with_bitvec(data, bitvec))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}
}
