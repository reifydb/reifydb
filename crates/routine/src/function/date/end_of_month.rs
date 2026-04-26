// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DateEndOfMonth {
	info: RoutineInfo,
}

impl Default for DateEndOfMonth {
	fn default() -> Self {
		Self::new()
	}
}

impl DateEndOfMonth {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::end_of_month"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateEndOfMonth {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Date(container) => {
				let mut result = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = container.get(i) {
						let year = date.year();
						let month = date.month();
						let last_day = Date::days_in_month(year, month);
						match Date::new(year, month, last_day) {
							Some(d) => result.push(d),
							None => result.push_default(),
						}
					} else {
						result.push_default();
					}
				}

				ColumnBuffer::Date(result)
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = if let Some(bv) = bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
	}
}
