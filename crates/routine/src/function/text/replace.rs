// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextReplace {
	info: FunctionInfo,
}

impl Default for TextReplace {
	fn default() -> Self {
		Self::new()
	}
}

impl TextReplace {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::replace"),
		}
	}
}

impl Function for TextReplace {
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
		if args.len() != 3 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let str_col = &args[0];
		let from_col = &args[1];
		let to_col = &args[2];

		let (str_data, str_bv) = str_col.data().unwrap_option();
		let (from_data, from_bv) = from_col.data().unwrap_option();
		let (to_data, to_bv) = to_col.data().unwrap_option();
		let row_count = str_data.len();

		match (str_data, from_data, to_data) {
			(
				ColumnBuffer::Utf8 {
					container: str_container,
					..
				},
				ColumnBuffer::Utf8 {
					container: from_container,
					..
				},
				ColumnBuffer::Utf8 {
					container: to_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if str_container.is_defined(i)
						&& from_container.is_defined(i) && to_container.is_defined(i)
					{
						let s = &str_container[i];
						let from = &from_container[i];
						let to = &to_container[i];
						result_data.push(s.replace(from.as_str(), to.as_str()));
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				};

				// Combine all three bitvecs
				let mut combined_bv: Option<BitVec> = None;
				for bv in [str_bv, from_bv, to_bv].into_iter().flatten() {
					combined_bv = Some(match combined_bv {
						Some(existing) => existing.and(bv),
						None => bv.clone(),
					});
				}

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
				ColumnBuffer::Utf8 {
					..
				},
				other,
			) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(
				ColumnBuffer::Utf8 {
					..
				},
				other,
				_,
			) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _, _) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
