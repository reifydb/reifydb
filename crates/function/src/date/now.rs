// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct DateNow;

impl DateNow {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateNow {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let row_count = ctx.row_count;

		if ctx.columns.len() != 0 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let millis = ctx.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis);
		let date = dt.date();

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(date);
		}

		Ok(ColumnData::Date(container))
	}
}
