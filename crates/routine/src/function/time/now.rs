// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::time_array, datetime::DateTime, value_type::ValueType};

pub struct TimeNow {
	info: RoutineInfo,
}

impl Default for TimeNow {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeNow {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeNow {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, _args: &Columns) -> Result<Columns, RoutineError> {
		let millis = ctx.runtime_context.clock.now().to_millis();
		let dt = DateTime::from_epoch_millis(millis)?;
		let time = dt.time();

		let mut container = Vec::with_capacity(1);
		container.push(time);

		let result_data = ColumnBuffer::Time(time_array(container));
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for TimeNow {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
