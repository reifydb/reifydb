// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct TextChar;

impl TextChar {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextChar {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let col = columns.get(0).unwrap();

		match col.data() {
			ColumnData::Int1(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Int2(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Int4(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Int8(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Uint1(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Uint2(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			ColumnData::Uint4(c) => {
				convert_to_char(row_count, c.data().len(), |i| c.get(i).map(|&v| v as u32))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Int1, Type::Int2, Type::Int4, Type::Int8],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}

fn convert_to_char<F>(row_count: usize, _capacity: usize, get_value: F) -> ScalarFunctionResult<ColumnData>
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

	Ok(ColumnData::Utf8 {
		container: Utf8Container::new(result_data),
		max_bytes: MaxBytes::MAX,
	})
}
