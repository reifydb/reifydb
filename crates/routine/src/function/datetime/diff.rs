// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::temporal_array::{datetimes, duration_array},
	duration::Duration,
	value_type::ValueType,
};

pub struct DateTimeDiff {
	info: RoutineInfo,
}

impl Default for DateTimeDiff {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeDiff {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::diff"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeDiff {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data1 = &args[0];
		let data2 = &args[1];
		let row_count = data1.len();

		let result_data = match (data1, data2) {
			(ColumnBuffer::DateTime(container1), ColumnBuffer::DateTime(container2)) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (datetimes(container1).get(i), datetimes(container2).get(i)) {
						(Some(dt1), Some(dt2)) => {
							let diff_nanos = dt1
								.to_nanos()
								.checked_sub(dt2.to_nanos())
								.ok_or_else(|| {
									RoutineError::FunctionExecutionFailed {
									function: ctx.fragment.clone(),
									reason: "datetime difference out of range".to_string(),
								}
								})?;
							container.push(Duration::from_nanoseconds(diff_nanos)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				ColumnBuffer::Duration(duration_array(container))
			}
			(ColumnBuffer::DateTime(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::DateTime],
					actual: other.get_type(),
				});
			}
			(other, _) => {
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

impl Function for DateTimeDiff {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
