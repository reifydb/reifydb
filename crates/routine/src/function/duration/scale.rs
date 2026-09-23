// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use crate::function::support::coerce::read_i64;
use reifydb_value::value::{
	container::temporal_array::{duration_array, durations},
	duration::Duration,
	value_type::ValueType,
};

pub struct DurationScale {
	info: RoutineInfo,
}

impl Default for DurationScale {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationScale {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::scale"),
		}
	}
}

fn is_integer_type(data: &ColumnBuffer) -> bool {
	matches!(
		data,
		ColumnBuffer::Int1(_)
			| ColumnBuffer::Int2(_)
			| ColumnBuffer::Int4(_)
			| ColumnBuffer::Int8(_)
			| ColumnBuffer::Int16(_)
			| ColumnBuffer::Uint1(_)
			| ColumnBuffer::Uint2(_)
			| ColumnBuffer::Uint4(_)
			| ColumnBuffer::Uint8(_)
			| ColumnBuffer::Uint16(_)
	)
}

impl<'a> Routine<FunctionContext<'a>> for DurationScale {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let dur_data = &args[0];
		let scalar_data = &args[1];

		match dur_data {
			ColumnBuffer::Duration(dur_container) => {
				if !is_integer_type(scalar_data) {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![
							ValueType::Int1,
							ValueType::Int2,
							ValueType::Int4,
							ValueType::Int8,
							ValueType::Int16,
							ValueType::Uint1,
							ValueType::Uint2,
							ValueType::Uint4,
							ValueType::Uint8,
							ValueType::Uint16,
						],
						actual: scalar_data.get_type(),
					});
				}

				let row_count = dur_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (durations(dur_container).get(i), read_i64(&ctx.fragment, scalar_data, i)?) {
						(Some(dur), Some(scalar)) => {
							container.push(*dur * scalar);
						}
						_ => container.push(Duration::default()),
					}
				}

				let result_data = ColumnBuffer::Duration(duration_array(container));
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

impl Function for DurationScale {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
