// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::datetimes, value_type::ValueType};

pub struct DateTimeEpochMillis {
	info: RoutineInfo,
}

impl Default for DateTimeEpochMillis {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeEpochMillis {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::epoch_millis"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeEpochMillis {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::DateTime(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = datetimes(container).get(i) {
						result.push(dt.to_epoch_millis());
						res_bitvec.push(true);
					} else {
						result.push(0);
						res_bitvec.push(false);
					}
				}

				ColumnBuffer::int8_with_bitvec(result, res_bitvec)
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: other.get_type(),
				});
			}
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for DateTimeEpochMillis {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
