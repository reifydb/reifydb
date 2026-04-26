// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DateTimeTime {
	info: RoutineInfo,
}

impl Default for DateTimeTime {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeTime {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::time"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeTime {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
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
			ColumnBuffer::DateTime(container) => {
				let mut result = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = container.get(i) {
						result.push(dt.time());
					} else {
						result.push_default();
					}
				}

				ColumnBuffer::Time(result)
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::DateTime],
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
