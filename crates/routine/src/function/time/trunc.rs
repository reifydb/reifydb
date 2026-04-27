// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TimeTrunc {
	info: RoutineInfo,
}

impl Default for TimeTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let time_col = &args[0];
		let prec_col = &args[1];

		let (time_data, time_bv) = time_col.unwrap_option();
		let (prec_data, _) = prec_col.unwrap_option();

		match (time_data, prec_data) {
			(
				ColumnBuffer::Time(time_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let row_count = time_data.len();
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
							match truncated {
								Some(val) => container.push(val),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				let mut result_data = ColumnBuffer::Time(container);
				if let Some(bv) = time_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Time(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TimeTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
