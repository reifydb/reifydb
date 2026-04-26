// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DurationNegate {
	info: RoutineInfo,
}

impl Default for DurationNegate {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationNegate {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::negate"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationNegate {
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

		match data {
			ColumnBuffer::Duration(container_in) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(val) = container_in.get(i) {
						container.push(val.negate());
					} else {
						container.push_default();
					}
				}

				let mut result_data = ColumnBuffer::Duration(container);
				if let Some(bv) = bitvec {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}
