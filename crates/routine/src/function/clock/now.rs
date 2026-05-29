// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::value_type::ValueType;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct Now {
	info: RoutineInfo,
}

impl Default for Now {
	fn default() -> Self {
		Self::new()
	}
}

impl Now {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("clock::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Now {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let millis = ctx.runtime_context.clock.now_millis() as i64;
		let row_count = ctx.row_count.max(1);
		let data = vec![millis; row_count];
		let bitvec = vec![true; row_count];

		let result_data = ColumnBuffer::int8_with_bitvec(data, bitvec);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for Now {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
