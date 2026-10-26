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
	constraint::bytes::MaxBytes,
	value_type::ValueType,
};

use crate::function::support::column::utf8_column;

pub struct TextConcat {
	info: RoutineInfo,
}

impl Default for TextConcat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextConcat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::concat"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextConcat {
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
		let mut containers = Vec::with_capacity(args.len());

		for (idx, col) in args.iter().enumerate() {
			let view = ColumnView::try_from(col)?;
			match &view.data {
				ViewData::Utf8 {
					container,
					..
				} => containers.push(*container),
				_ => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: idx,
						expected: vec![ValueType::Utf8],
						actual: view.get_type(),
					});
				}
			}
		}

		let row_count = ctx.row_count;
		let mut result = Vec::with_capacity(row_count);
		for i in 0..row_count {
			let mut value = String::new();
			let mut valid = true;
			for container in &containers {
				let row = row_of(container.len(), i);
				if container.is_null(row) {
					valid = false;
					break;
				}
				value.push_str(container.value(row));
			}
			result.push(valid.then_some(value));
		}
		let container = LargeStringArray::from(result);

		Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, container))
	}
}

impl Function for TextConcat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::AtLeast(2)
	}
}
