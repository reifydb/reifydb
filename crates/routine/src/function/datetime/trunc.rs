// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DateTimeTrunc {
	info: RoutineInfo,
}

impl Default for DateTimeTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let dt_col = &args[0];
		let prec_col = &args[1];
		let (dt_data, dt_bitvec) = dt_col.unwrap_option();
		let (prec_data, prec_bitvec) = prec_col.unwrap_option();
		let row_count = dt_data.len();

		let result_data = match (dt_data, prec_data) {
			(
				ColumnBuffer::DateTime(dt_container),
				ColumnBuffer::Utf8 {
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
									return Err(RoutineError::FunctionExecutionFailed {
										function: ctx.env.fragment.clone(),
										reason: format!(
											"invalid precision: '{}'",
											other
										),
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

				ColumnBuffer::DateTime(container)
			}
			(ColumnBuffer::DateTime(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Utf8],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (dt_bitvec, prec_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
	}
}
