// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TimeAge {
	info: RoutineInfo,
}

impl Default for TimeAge {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeAge {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::age"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeAge {
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

		let (data1, bv1) = col1.unwrap_option();
		let (data2, bv2) = col2.unwrap_option();

		match (data1, data2) {
			(ColumnBuffer::Time(container1), ColumnBuffer::Time(container2)) => {
				let row_count = data1.len();
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(t1), Some(t2)) => {
							let diff_nanos = t1.to_nanos_since_midnight() as i64
								- t2.to_nanos_since_midnight() as i64;
							container.push(Duration::from_nanoseconds(diff_nanos)?);
						}
						_ => container.push_default(),
					}
				}

				let mut result_data = ColumnBuffer::Duration(container);
				if let Some(bv) = bv1 {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				} else if let Some(bv) = bv2 {
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
				expected: vec![Type::Time],
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

impl Function for TimeAge {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
