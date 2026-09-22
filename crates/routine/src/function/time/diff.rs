// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::temporal_array::{duration_array, times},
	duration::Duration,
	value_type::ValueType,
};

pub struct TimeDiff {
	info: RoutineInfo,
}

impl Default for TimeDiff {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeDiff {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::diff"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeDiff {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let col1 = &args[0];
		let col2 = &args[1];

		let (data1, bv1) = col1.unwrap_option();
		let (data2, bv2) = col2.unwrap_option();

		match (data1, data2) {
			(ColumnBuffer::Time(container1), ColumnBuffer::Time(container2)) => {
				let row_count = data1.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (times(container1).get(i), times(container2).get(i)) {
						(Some(t1), Some(t2)) => {
							let diff_nanos = t1.to_nanos_since_midnight() as i64
								- t2.to_nanos_since_midnight() as i64;
							container.push(Duration::from_nanoseconds(diff_nanos)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				let mut result_data = ColumnBuffer::Duration(duration_array(container));
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
				expected: vec![ValueType::Time],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Time],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TimeDiff {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
