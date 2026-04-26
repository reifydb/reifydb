// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DateNow {
	info: RoutineInfo,
}

impl Default for DateNow {
	fn default() -> Self {
		Self::new()
	}
}

impl DateNow {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateNow {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let row_count = args.row_count().max(1);

		let millis = ctx.env.runtime_context.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis)?;
		let date = dt.date();

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(date);
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), ColumnBuffer::Date(container))]))
	}
}
