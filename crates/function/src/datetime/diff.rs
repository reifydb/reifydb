// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct DateTimeDiff;

impl DateTimeDiff {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeDiff {
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
			(ColumnData::DateTime(container1), ColumnData::DateTime(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(dt1), Some(dt2)) => {
							let diff_nanos =
								dt1.to_nanos_since_epoch() - dt2.to_nanos_since_epoch();
							container.push(Duration::from_nanoseconds(diff_nanos));
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::DateTime],
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
}
