// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct TimeTrunc;

impl TimeTrunc {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TimeTrunc {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
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
		let prec_col = columns.get(1).unwrap();

		match (time_col.data(), prec_col.data()) {
			(
				ColumnData::Time(time_container),
				ColumnData::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (time_container.get(i), prec_container.is_defined(i)) {
						(Some(t), true) => {
							let precision = &prec_container[i];
							let truncated = match precision.as_str() {
								"hour" => Time::new(t.hour(), 0, 0, 0),
								"minute" => Time::new(t.hour(), t.minute(), 0, 0),
								"second" => {
									Time::new(t.hour(), t.minute(), t.second(), 0)
								}
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
				expected: vec![Type::Utf8],
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
