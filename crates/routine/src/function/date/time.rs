// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, columns::Columns, buffer::ColumnBuffer};
use reifydb_type::value::r#type::{Type, input_types::InputTypes};

use crate::function::{
	Function, FunctionCapability, FunctionContext, FunctionInfo,
	error::{ScalarFunctionResult, RoutineError},
};

pub struct DateTime;

impl Default for DateTime {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTime {
	pub fn new() -> Self {
		Self
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTime {
	fn info(&self) -> &RoutineInfo {
		static INFO: FunctionInfo = FunctionInfo {
			name: "date::time".to_string(),
			description: None,
		};
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::DateTime
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> ScalarFunctionResult<Columns> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, _bitvec) = column.data().unwrap_option();
		let row_count = data.len();

		if !data.get_type().is_datetime() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::DateTime.expected_at(0).to_vec(),
				actual: data.get_type(),
			});
		}

		let mut result_data = ColumnBuffer::time_with_capacity(row_count);

		for i in 0..row_count {
			if data.is_defined(i) {
				let datetime_val = data.get_value(i);
				result_data.push(datetime_val.time());
			} else {
				result_data.push(Value::null());
			}
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for DateTime {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
