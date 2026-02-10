// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct DateTimeTrunc;

impl DateTimeTrunc {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeTrunc {
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

		let dt_col = columns.get(0).unwrap();
		let prec_col = columns.get(1).unwrap();

		match (dt_col.data(), prec_col.data()) {
			(
				ColumnData::DateTime(dt_container),
				ColumnData::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), prec_container.is_defined(i)) {
						(Some(dt), true) => {
							let precision = &prec_container[i];
							let truncated = match precision.as_str() {
								"year" => DateTime::new(dt.year(), 1, 1, 0, 0, 0, 0),
								"month" => DateTime::new(
									dt.year(),
									dt.month(),
									1,
									0,
									0,
									0,
									0,
								),
								"day" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									0,
									0,
									0,
									0,
								),
								"hour" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									0,
									0,
									0,
								),
								"minute" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									dt.minute(),
									0,
									0,
								),
								"second" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									dt.minute(),
									dt.second(),
									0,
								),
								other => {
									return Err(
										ScalarFunctionError::ExecutionFailed {
											function: ctx.fragment.clone(),
											reason: format!(
												"invalid precision: '{}'",
												other
											),
										},
									);
								}
							};
							match truncated {
								Some(val) => container.push(val),
								None => container.push_undefined(),
							}
						}
						_ => container.push_undefined(),
					}
				}

				Ok(ColumnData::DateTime(container))
			}
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
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
