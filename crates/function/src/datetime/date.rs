// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct DateTimeDate;

impl DateTimeDate {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeDate {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
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
				let mut result = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = container.get(i) {
						result.push(dt.date());
					} else {
						result.push_default();
					}
				}

				Ok(ColumnData::Date(result))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}
}
