// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct TextIndexOf;

impl TextIndexOf {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextIndexOf {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let str_col = columns.get(0).unwrap();
		let substr_col = columns.get(1).unwrap();

		match (str_col.data(), substr_col.data()) {
			(
				ColumnData::Utf8 {
					container: str_container,
					..
				},
				ColumnData::Utf8 {
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

				Ok(ColumnData::int4_with_bitvec(result_data, result_bitvec))
			}
			(
				ColumnData::Utf8 {
					..
				},
				other,
			) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
