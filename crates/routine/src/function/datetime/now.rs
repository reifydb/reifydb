// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal::TemporalContainer, datetime::DateTime, value_type::ValueType};

pub struct DateTimeNow {
	info: RoutineInfo,
}

impl Default for DateTimeNow {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeNow {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeNow {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::DateTime
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let row_count = args.row_count().max(1);

		let millis = ctx.runtime_context.clock.now().to_millis();
		let dt = DateTime::from_epoch_millis(millis)?;

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(dt);
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::DateTime(container))]))
	}
}

impl Function for DateTimeNow {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
