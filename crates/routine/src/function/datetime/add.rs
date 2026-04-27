// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateTimeAdd {
	info: RoutineInfo,
}

impl Default for DateTimeAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeAdd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::add"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeAdd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let dt_col = &args[0];
		let dur_col = &args[1];
		let (dt_data, dt_bitvec) = dt_col.unwrap_option();
		let (dur_data, dur_bitvec) = dur_col.unwrap_option();
		let row_count = dt_data.len();

		let result_data = match (dt_data, dur_data) {
			(ColumnBuffer::DateTime(dt_container), ColumnBuffer::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), dur_container.get(i)) {
						(Some(dt), Some(dur)) => match dt.add_duration(dur) {
							Ok(result) => container.push(result),
							Err(err) => {
								return Err(RoutineError::FunctionExecutionFailed {
									function: ctx.fragment.clone(),
									reason: format!("{}", err),
								});
							}
						},
						_ => container.push_default(),
					}
				}

				ColumnBuffer::DateTime(container)
			}
			(ColumnBuffer::DateTime(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Duration],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (dt_bitvec, dur_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateTimeAdd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
