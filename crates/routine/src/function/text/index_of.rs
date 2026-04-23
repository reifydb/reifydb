// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextIndexOf {
	info: FunctionInfo,
}

impl Default for TextIndexOf {
	fn default() -> Self {
		Self::new()
	}
}

impl TextIndexOf {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::index_of"),
		}
	}
}

impl Function for TextIndexOf {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let str_col = &args[0];
		let substr_col = &args[1];

		let (str_data, str_bv) = str_col.data().unwrap_option();
		let (substr_data, substr_bv) = substr_col.data().unwrap_option();
		let row_count = str_data.len();

		match (str_data, substr_data) {
			(
				ColumnBuffer::Utf8 {
					container: str_container,
					..
				},
				ColumnBuffer::Utf8 {
					container: substr_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if str_container.is_defined(i) && substr_container.is_defined(i) {
						let s = &str_container[i];
						let substr = &substr_container[i];
						let index = s
							.find(substr.as_str())
							.map(|pos| {
								// Convert byte position to character position
								s[..pos].chars().count() as i32
							})
							.unwrap_or(-1);
						result_data.push(index);
						result_bitvec.push(true);
					} else {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}

				let result_col_data = ColumnBuffer::int4_with_bitvec(result_data, result_bitvec);

				let combined_bv = match (str_bv, substr_bv) {
					(Some(b), Some(e)) => Some(b.and(e)),
					(Some(b), None) => Some(b.clone()),
					(None, Some(e)) => Some(e.clone()),
					(None, None) => None,
				};

				let final_data = match combined_bv {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv,
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			(
				ColumnBuffer::Utf8 {
					..
				},
				other,
			) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
