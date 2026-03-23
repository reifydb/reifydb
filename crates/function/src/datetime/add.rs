// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct DateTimeAdd;

impl DateTimeAdd {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeAdd {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
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

		let dt_col = columns.get(0).unwrap();
		let dur_col = columns.get(1).unwrap();

		match (dt_col.data(), dur_col.data()) {
			(ColumnData::DateTime(dt_container), ColumnData::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), dur_container.get(i)) {
						(Some(dt), Some(dur)) => match dt.add_duration(&dur) {
							Ok(result) => container.push(result),
							Err(_) => container.push_default(),
						},
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::DateTime(container))
			}
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}
}
