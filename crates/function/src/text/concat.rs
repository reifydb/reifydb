// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct TextConcat;

impl TextConcat {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextConcat {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() < 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		// Validate all arguments are Utf8
		for (idx, col) in columns.iter().enumerate() {
			match col.data() {
				ColumnData::Utf8 {
					..
				} => {}
				other => {
					return Err(ScalarFunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: idx,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let mut result_data = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let mut all_defined = true;
			let mut concatenated = String::new();

			for col in columns.iter() {
				if let ColumnData::Utf8 {
					container,
					..
				} = col.data()
				{
					if container.is_defined(i) {
						concatenated.push_str(&container[i]);
					} else {
						all_defined = false;
						break;
					}
				}
			}

			if all_defined {
				result_data.push(concatenated);
			} else {
				result_data.push(String::new());
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		})
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
