// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::duration;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{temporal_array::durations, varlen_array},
	duration::Duration,
	value_type::ValueType,
};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let dur_data = ColumnView::try_from(&args[0])?;
		let prec_data = ColumnView::try_from(&args[1])?;

		match (&dur_data.data, &prec_data.data) {
			(
				ViewData::Duration(dur_container),
				ViewData::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let row_count = dur_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (durations(dur_container).get(i), varlen_array::get(prec_container, i)) {
						(Some(dur), Some(precision)) => {
							let months = dur.get_months();
							let days = dur.get_days();
							let nanos = dur.get_nanos();

							let truncated = match precision {
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
						_ => container.push(Duration::default()),
					}
				}

				let result_data = duration(ctx.fragment.text(), container);
				Ok(result_data)
			}
			(ViewData::Duration(_), _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: prec_data.get_type(),
			}),
			(_, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: dur_data.get_type(),
			}),
		}
	}
}

impl Function for DurationTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
