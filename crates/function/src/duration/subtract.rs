// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct DurationSubtract;

impl DurationSubtract {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DurationSubtract {
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

		let lhs_col = columns.get(0).unwrap();
		let rhs_col = columns.get(1).unwrap();

		match (lhs_col.data(), rhs_col.data()) {
			(ColumnData::Duration(lhs_container), ColumnData::Duration(rhs_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (lhs_container.get(i), rhs_container.get(i)) {
						(Some(lv), Some(rv)) => {
							container.push(*lv - *rv);
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::Duration(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}
