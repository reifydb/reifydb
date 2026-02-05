// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, ScalarFunctionError};

pub struct TextTrim;

impl TextTrim {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextTrim {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate exactly 1 argument
		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let original_str = &container[i];
						let trimmed_str = original_str.trim();
						result_data.push(trimmed_str.to_string());
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: *max_bytes,
				})
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
