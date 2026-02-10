// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct DateStartOfMonth;

impl DateStartOfMonth {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateStartOfMonth {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
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
				let mut result = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = container.get(i) {
						match Date::new(date.year(), date.month(), 1) {
							Some(d) => result.push(d),
							None => result.push_undefined(),
						}
					} else {
						result.push_undefined();
					}
				}

				Ok(ColumnData::Date(result))
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
