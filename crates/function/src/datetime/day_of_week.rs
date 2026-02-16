// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTimeDayOfWeek;

impl DateTimeDayOfWeek {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeDayOfWeek {
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
			ColumnData::DateTime(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = container.get(i) {
						let date = dt.date();
						// ISO 8601: Mon=1, Sun=7
						// 1970-01-01 was Thursday (ISO day 4), so days_since_epoch 0 = Thursday
						// (days + 3) % 7 shifts Thursday=0 to Monday=0 base
						// +7) % 7 handles negative days, +1 converts to 1-based
						let days = date.to_days_since_epoch();
						let dow = ((days % 7 + 3) % 7 + 7) % 7 + 1;
						data.push(dow);
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
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
		}
	}
}
