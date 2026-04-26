// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

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

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let millis = ctx.env.runtime_context.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis)?;
		let time = dt.time();

		// For zero-arg functions, we produce a single row
		let mut container = TemporalContainer::with_capacity(1);
		container.push(time);

		let result_data = ColumnBuffer::Time(container);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
	}
}
