// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextStartsWith {
	info: FunctionInfo,
}

impl Default for TextStartsWith {
	fn default() -> Self {
		Self::new()
	}
}

impl TextStartsWith {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::starts_with"),
		}
	}
}

impl Function for TextStartsWith {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
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
		let prefix_col = &args[1];

		let (str_data, str_bv) = str_col.data().unwrap_option();
		let (prefix_data, prefix_bv) = prefix_col.data().unwrap_option();
		let row_count = str_data.len();

		match (str_data, prefix_data) {
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

				let result_col_data = ColumnData::bool_with_bitvec(result_data, result_bitvec);

				let combined_bv = match (str_bv, prefix_bv) {
					(Some(b), Some(e)) => Some(b.and(e)),
					(Some(b), None) => Some(b.clone()),
					(None, Some(e)) => Some(e.clone()),
					(None, None) => None,
				};

				let final_data = match combined_bv {
					Some(bv) => ColumnData::Option {
						inner: Box::new(result_col_data),
						bitvec: bv,
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
			}
			(
				ColumnData::Utf8 {
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
