// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateIsLeapYear;

impl DateIsLeapYear {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateIsLeapYear {
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
						data.push(Date::is_leap_year(date.year()));
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::bool_with_bitvec(data, bitvec))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}
}
