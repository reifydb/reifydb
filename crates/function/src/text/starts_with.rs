// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct TextStartsWith;

impl TextStartsWith {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextStartsWith {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

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
		let prefix_col = columns.get(1).unwrap();

		match (str_col.data(), prefix_col.data()) {
			(
				ColumnData::Utf8 {
					container: str_container,
					..
				},
				ColumnData::Utf8 {
					container: prefix_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if str_container.is_defined(i) && prefix_container.is_defined(i) {
						let s = &str_container[i];
						let prefix = &prefix_container[i];
						result_data.push(s.starts_with(prefix.as_str()));
						result_bitvec.push(true);
					} else {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::bool_with_bitvec(result_data, result_bitvec))
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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}
}
