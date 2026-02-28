// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct DurationTrunc;

impl DurationTrunc {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DurationTrunc {
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

		let dur_col = columns.get(0).unwrap();
		let prec_col = columns.get(1).unwrap();

		match (dur_col.data(), prec_col.data()) {
			(
				ColumnData::Duration(dur_container),
				ColumnData::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), prec_container.is_defined(i)) {
						(Some(dur), true) => {
							let precision = &prec_container[i];
							let months = dur.get_months();
							let days = dur.get_days();
							let nanos = dur.get_nanos();

							let truncated = match precision.as_str() {
								"year" => Duration::new((months / 12) * 12, 0, 0),
								"month" => Duration::new(months, 0, 0),
								"day" => Duration::new(months, days, 0),
								"hour" => Duration::new(
									months,
									days,
									(nanos / 3_600_000_000_000) * 3_600_000_000_000,
								),
								"minute" => Duration::new(
									months,
									days,
									(nanos / 60_000_000_000) * 60_000_000_000,
								),
								"second" => Duration::new(
									months,
									days,
									(nanos / 1_000_000_000) * 1_000_000_000,
								),
								"millis" => Duration::new(
									months,
									days,
									(nanos / 1_000_000) * 1_000_000,
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
							container.push(truncated);
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			(ColumnData::Duration(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}
}
