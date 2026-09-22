// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::temporal_array::{duration_array, durations},
	duration::Duration,
	value_type::ValueType,
};

pub struct DurationSubtract {
	info: RoutineInfo,
}

impl Default for DurationSubtract {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationSubtract {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::subtract"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationSubtract {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let lhs_data = &args[0];
		let rhs_data = &args[1];

		match (lhs_data, rhs_data) {
			(ColumnBuffer::Duration(lhs_container), ColumnBuffer::Duration(rhs_container)) => {
				let row_count = lhs_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (durations(lhs_container).get(i), durations(rhs_container).get(i)) {
						(Some(lv), Some(rv)) => {
							container.push(*lv - *rv);
						}
						_ => container.push(Duration::default()),
					}
				}

				let result_data = ColumnBuffer::Duration(duration_array(container));
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Duration(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for DurationSubtract {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
