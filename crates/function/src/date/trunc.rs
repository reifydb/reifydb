// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTrunc;

impl DateTrunc {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTrunc {
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

		let date_col = columns.get(0).unwrap();
		let prec_col = columns.get(1).unwrap();

		match (date_col.data(), prec_col.data()) {
			(
				ColumnData::Date(date_container),
				ColumnData::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), prec_container.is_defined(i)) {
						(Some(d), true) => {
							let precision = &prec_container[i];
							let truncated =
								match precision.as_str() {
									"year" => Date::new(d.year(), 1, 1),
									"month" => Date::new(d.year(), d.month(), 1),
									other => {
										return Err(ScalarFunctionError::ExecutionFailed {
										function: ctx.fragment.clone(),
										reason: format!("invalid precision: '{}'", other),
									});
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

				Ok(ColumnData::Date(container))
			}
			(ColumnData::Date(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}
}
