// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{column_view::ColumnView, constraint::bytes::MaxBytes, value_type::ValueType};

use crate::function::support::column::utf8_column;

pub struct Type {
	info: RoutineInfo,
}

impl Default for Type {
	fn default() -> Self {
		Self::new()
	}
}

impl Type {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("meta::type"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Type {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
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
		let col_type = column.get_type();
		let type_name = col_type.to_string();
		let row_count = column.len();

		let result_data: Vec<String> = vec![type_name; row_count];

		Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, LargeStringArray::from(result_data)))
	}
}

impl Function for Type {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
