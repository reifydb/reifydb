// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::{container::temporal::TemporalContainer, datetime::DateTime, value_type::ValueType};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Date
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
		let date = dt.date();

		let mut container = TemporalContainer::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(date);
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::Date(container))]))
	}
}

impl Function for DateNow {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
