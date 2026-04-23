// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, columns::Columns, buffer::ColumnBuffer};
use reifydb_type::value::r#type::{Type, input_types::InputTypes};

use crate::function::{
	Function, FunctionCapability, FunctionContext, FunctionInfo,
	error::{ScalarFunctionResult, FunctionError},
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

impl Function for DateTime {
	fn info(&self) -> &FunctionInfo {
		static INFO: FunctionInfo = FunctionInfo {
			name: "date::time".to_string(),
			description: None,
		};
		&INFO
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time // Returns the Time part of a DateTime
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::DateTime
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> ScalarFunctionResult<Columns> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, _bitvec) = column.data().unwrap_option();
		let row_count = data.len();

		if !data.get_type().is_datetime() {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::DateTime.expected_at(0).to_vec(),
				actual: data.get_type(),
			});
		}

		let mut result_data = ColumnBuffer::time_with_capacity(row_count);

		for i in 0..row_count {
			if data.is_defined(i) {
				let datetime_val = data.get_value(i); // Assuming get_value returns a DateTime type
				result_data.push(datetime_val.time());
			} else {
				result_data.push(Value::null()); // Placeholder for undefined
			}
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}
