// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let row_count = args.row_count().max(1);

		let millis = ctx.runtime_context.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis)?;

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
}
