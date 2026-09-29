// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{column_view::ColumnView, value_type::ValueType};

use crate::function::support::column::array_column;

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let column = ColumnView::try_from(&args[0])?;
		let values = match column.logical_nulls() {
			None => BooleanBuffer::new_unset(column.len()),
			Some(nulls) => !nulls.inner(),
		};

		Ok(array_column(ctx.fragment.text(), ValueType::Boolean, Arc::new(BooleanArray::new(values, None))))
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
