// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, broadcast::row_of, context::FunctionContext,
	error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

use crate::function::support::column::utf8_column;

pub struct TextTrimEnd {
	info: RoutineInfo,
}

impl Default for TextTrimEnd {
	fn default() -> Self {
		Self::new()
	}
}

impl TextTrimEnd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::trim_end"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextTrimEnd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = ctx.row_count;

		match &data.data {
			ViewData::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					let original_str = container.value(row_of(container.len(), i));
					let trimmed_str = original_str.trim_end();
					result_data.push(trimmed_str.to_string());
				}

				Ok(utf8_column(ctx.fragment.text(), *max_bytes, LargeStringArray::from(result_data)))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: data.get_type(),
			}),
		}
	}
}

impl Function for TextTrimEnd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
