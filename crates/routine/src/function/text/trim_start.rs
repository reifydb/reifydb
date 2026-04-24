// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::utf8::Utf8Container, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextTrimStart {
	info: FunctionInfo,
}

impl Default for TextTrimStart {
	fn default() -> Self {
		Self::new()
	}
}

impl TextTrimStart {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::trim_start"),
		}
	}
}

impl Function for TextTrimStart {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
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
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let original_str = &container[i];
						let trimmed_str = original_str.trim_start();
						result_data.push(trimmed_str.to_string());
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: *max_bytes,
				};
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv.clone(),
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
