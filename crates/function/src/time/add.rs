// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct TimeAdd;

impl TimeAdd {
	pub fn new() -> Self {
		Self
	}
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl ScalarFunction for TimeAdd {
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

		let time_col = columns.get(0).unwrap();
		let dur_col = columns.get(1).unwrap();

		match (time_col.data(), dur_col.data()) {
			(ColumnData::Time(time_container), ColumnData::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (time_container.get(i), dur_container.get(i)) {
						(Some(time), Some(dur)) => {
							let time_nanos = time.to_nanos_since_midnight() as i64;
							let dur_nanos =
								dur.get_nanos() + dur.get_days() as i64 * NANOS_PER_DAY;

							let result_nanos =
								(time_nanos + dur_nanos).rem_euclid(NANOS_PER_DAY);
							match Time::from_nanos_since_midnight(result_nanos as u64) {
								Some(result) => container.push(result),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::Time(container))
			}
			(ColumnData::Time(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}
}
