// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DurationAdd {
	info: RoutineInfo,
}

impl Default for DurationAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationAdd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::add"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationAdd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let lhs_col = &args[0];
		let rhs_col = &args[1];

		let (lhs_data, lhs_bv) = lhs_col.unwrap_option();
		let (rhs_data, rhs_bv) = rhs_col.unwrap_option();

		match (lhs_data, rhs_data) {
			(ColumnBuffer::Duration(lhs_container), ColumnBuffer::Duration(rhs_container)) => {
				let row_count = lhs_data.len();
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (lhs_container.get(i), rhs_container.get(i)) {
						(Some(lv), Some(rv)) => {
							container.push(*lv + *rv);
						}
						_ => container.push_default(),
					}
				}

				let mut result_data = ColumnBuffer::Duration(container);
				if let Some(bv) = lhs_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				} else if let Some(bv) = rhs_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Duration(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}
