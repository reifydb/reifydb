// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext, ScalarFunctionResult, error::ScalarFunctionError, propagate_options,
};

pub struct DateTimeNow;

impl DateTimeNow {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeNow {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
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

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(dt);
		}

		Ok(ColumnData::DateTime(container))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}
}
