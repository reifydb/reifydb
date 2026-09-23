// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct IsNone {
	info: RoutineInfo,
}

impl Default for IsNone {
	fn default() -> Self {
		Self::new()
	}
}

impl IsNone {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::none"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsNone {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Boolean
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let column = &args[0];
		let values = match column.nulls() {
			None => BooleanBuffer::new_unset(column.len()),
			Some(nulls) => !nulls.inner(),
		};

		Ok(Columns::new(vec![ColumnWithName::new(
			ctx.fragment.clone(),
			ColumnBuffer::Bool(BooleanArray::new(values, None)),
		)]))
	}
}

impl Function for IsNone {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
