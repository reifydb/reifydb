// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct TimeAge;

impl TimeAge {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TimeAge {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let col1 = columns.get(0).unwrap();
		let col2 = columns.get(1).unwrap();

		match (col1.data(), col2.data()) {
			(ColumnData::Time(container1), ColumnData::Time(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(t1), Some(t2)) => {
							let diff_nanos = t1.to_nanos_since_midnight() as i64
								- t2.to_nanos_since_midnight() as i64;
							container.push(Duration::from_nanoseconds(diff_nanos));
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::Time(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Time],
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
}
