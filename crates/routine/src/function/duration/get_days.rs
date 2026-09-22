// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::durations, value_type::ValueType};

pub struct DurationGetDays {
	info: RoutineInfo,
}

impl Default for DurationGetDays {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationGetDays {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::get_days"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationGetDays {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		match data {
			ColumnBuffer::Duration(container) => {
				let mut result = Vec::with_capacity(row_count);

				for dur in durations(container) {
					result.push(dur.get_days());
				}

				let result_data = ColumnBuffer::int4(result);
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for DurationGetDays {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
