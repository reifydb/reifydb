// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextChar {
	info: FunctionInfo,
}

impl Default for TextChar {
	fn default() -> Self {
		Self::new()
	}
}

impl TextChar {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::char"),
		}
	}
}

impl Function for TextChar {
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

		let result_data = match data {
			ColumnBuffer::Int1(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Int2(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Int4(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Int8(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Uint1(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Uint2(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnBuffer::Uint4(c) => convert_to_char(row_count, c.data().len(), |i| c.get(i).copied()),
			other => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Int1, Type::Int2, Type::Int4, Type::Int8],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match bitvec {
			Some(bv) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			None => result_data,
		};
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

fn convert_to_char<F>(row_count: usize, _capacity: usize, get_value: F) -> ColumnBuffer
where
	F: Fn(usize) -> Option<u32>,
{
	let mut result_data = Vec::with_capacity(row_count);

	for i in 0..row_count {
		match get_value(i) {
			Some(code_point) => {
				if let Some(ch) = char::from_u32(code_point) {
					result_data.push(ch.to_string());
				} else {
					result_data.push(String::new());
				}
			}
			None => {
				result_data.push(String::new());
			}
		}
	}

	ColumnBuffer::Utf8 {
		container: Utf8Container::new(result_data),
		max_bytes: MaxBytes::MAX,
	}
}
