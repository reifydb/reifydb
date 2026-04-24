// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type as ValueType};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Type {
	info: FunctionInfo,
}

impl Default for Type {
	fn default() -> Self {
		Self::new()
	}
}

impl Type {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("meta::type"),
		}
	}
}

impl Function for Type {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let col_type = column.get_type();
		let type_name = col_type.to_string();
		let row_count = column.len();

		let result_data: Vec<String> = vec![type_name; row_count];

		let final_data = ColumnBuffer::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
