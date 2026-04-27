// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateTimeNew {
	info: RoutineInfo,
}

impl Default for DateTimeNew {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeNew {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::new"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeNew {
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

		let date_col = &args[0];
		let time_col = &args[1];
		let (date_data, date_bitvec) = date_col.unwrap_option();
		let (time_data, time_bitvec) = time_col.unwrap_option();
		let row_count = date_data.len();

		let result_data = match (date_data, time_data) {
			(ColumnBuffer::Date(date_container), ColumnBuffer::Time(time_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), time_container.get(i)) {
						(Some(date), Some(time)) => {
							match DateTime::new(
								date.year(),
								date.month(),
								date.day(),
								time.hour(),
								time.minute(),
								time.second(),
								time.nanosecond(),
							) {
								Some(dt) => container.push(dt),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::DateTime(container)
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Time],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (date_bitvec, time_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateTimeNew {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
