// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DurationTrunc {
	info: RoutineInfo,
}

impl Default for DurationTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let dur_col = &args[0];
		let prec_col = &args[1];

		let (dur_data, dur_bv) = dur_col.unwrap_option();
		let (prec_data, _) = prec_col.unwrap_option();

		match (dur_data, prec_data) {
			(
				ColumnBuffer::Duration(dur_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let row_count = dur_data.len();
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), prec_container.is_defined(i)) {
						(Some(dur), true) => {
							let precision = &prec_container[i];
							let months = dur.get_months();
							let days = dur.get_days();
							let nanos = dur.get_nanos();

							let truncated = match precision.as_str() {
								"year" => Duration::new((months / 12) * 12, 0, 0)?,
								"month" => Duration::new(months, 0, 0)?,
								"day" => Duration::new(months, days, 0)?,
								"hour" => Duration::new(
									months,
									days,
									(nanos / 3_600_000_000_000) * 3_600_000_000_000,
								)?,
								"minute" => Duration::new(
									months,
									days,
									(nanos / 60_000_000_000) * 60_000_000_000,
								)?,
								"second" => Duration::new(
									months,
									days,
									(nanos / 1_000_000_000) * 1_000_000_000,
								)?,
								"millis" => Duration::new(
									months,
									days,
									(nanos / 1_000_000) * 1_000_000,
								)?,
								other => {
									return Err(
										RoutineError::FunctionExecutionFailed {
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

				let mut result_data = ColumnBuffer::Duration(container);
				if let Some(bv) = dur_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Duration(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for DurationTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
