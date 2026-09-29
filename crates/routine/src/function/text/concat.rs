// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use arrow_string::concat_elements::concat_elements_utf8_many;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
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

		let container = concat_elements_utf8_many(&containers).map_err(|err| {
			RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: err.to_string(),
			}
		})?;

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
