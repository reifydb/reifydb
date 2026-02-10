// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct TimeNow;

impl TimeNow {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TimeNow {
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
		let time = dt.time();

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(time);
		}

		Ok(ColumnData::Time(container))
	}
}
