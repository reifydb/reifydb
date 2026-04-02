// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::function::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct DateNow;

impl Default for DateNow {
	fn default() -> Self {
		Self::new()
	}
}

impl DateNow {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateNow {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let row_count = ctx.row_count;

		if !ctx.columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let millis = ctx.runtime_context.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis)?;
		let date = dt.date();

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(date);
		}

		Ok(ColumnData::Date(container))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}
}
