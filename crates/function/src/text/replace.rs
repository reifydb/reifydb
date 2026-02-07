// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct TextReplace;

impl TextReplace {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextReplace {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 3 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: columns.len(),
			});
		}

		let str_col = columns.get(0).unwrap();
		let from_col = columns.get(1).unwrap();
		let to_col = columns.get(2).unwrap();

		match (str_col.data(), from_col.data(), to_col.data()) {
			(
				ColumnData::Utf8 {
					container: str_container,
					..
				},
				ColumnData::Utf8 {
					container: from_container,
					..
				},
				ColumnData::Utf8 {
					container: to_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if str_container.is_defined(i)
						&& from_container.is_defined(i) && to_container.is_defined(i)
					{
						let s = &str_container[i];
						let from = &from_container[i];
						let to = &to_container[i];
						result_data.push(s.replace(from.as_str(), to.as_str()));
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: MaxBytes::MAX,
				})
			}
			(
				ColumnData::Utf8 {
					..
				},
				ColumnData::Utf8 {
					..
				},
				other,
			) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(
				ColumnData::Utf8 {
					..
				},
				other,
				_,
			) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
