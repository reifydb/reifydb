// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateDiff {
	info: RoutineInfo,
}

impl Default for DateDiff {
	fn default() -> Self {
		Self::new()
	}
}

impl DateDiff {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::diff"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateDiff {
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

		let col1 = &args[0];
		let col2 = &args[1];
		let (data1, bitvec1) = col1.unwrap_option();
		let (data2, bitvec2) = col2.unwrap_option();
		let row_count = data1.len();

		let result_data = match (data1, data2) {
			(ColumnBuffer::Date(container1), ColumnBuffer::Date(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(d1), Some(d2)) => {
							let diff_days = (d1.to_days_since_epoch()
								- d2.to_days_since_epoch()) as i64;
							container.push(Duration::from_days(diff_days)?);
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::Duration(container)
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Date],
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

		let final_data = match (bitvec1, bitvec2) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateDiff {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
