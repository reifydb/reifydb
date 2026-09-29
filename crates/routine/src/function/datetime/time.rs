// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::time;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::datetimes,
	time::Time,
	value_type::ValueType,
};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::DateTime(container) => {
				let mut result = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = datetimes(container).get(i) {
						result.push(dt.time());
					} else {
						result.push(Time::default());
					}
				}

				time(ctx.fragment.text(), result)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateTimeTime {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
