// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextRepeat {
	info: FunctionInfo,
}

impl Default for TextRepeat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextRepeat {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::repeat"),
		}
	}
}

impl Function for TextRepeat {
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
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let str_col = &args[0];
		let count_col = &args[1];

		let (str_data, str_bv) = str_col.data().unwrap_option();
		let (count_data, count_bv) = count_col.data().unwrap_option();
		let row_count = str_data.len();

		match str_data {
			ColumnData::Utf8 {
				container: str_container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if !str_container.is_defined(i) {
						result_data.push(String::new());
						continue;
					}

					let count = match count_data {
						ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int8(c) => c.get(i).copied(),
						ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
						_ => {
							return Err(FunctionError::InvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									Type::Int1,
									Type::Int2,
									Type::Int4,
									Type::Int8,
								],
								actual: count_data.get_type(),
							});
						}
					};

					match count {
						Some(n) if n >= 0 => {
							let s = &str_container[i];
							result_data.push(s.repeat(n as usize));
						}
						Some(_) => {
							result_data.push(String::new());
						}
						None => {
							result_data.push(String::new());
						}
					}
				}

				let result_col_data = ColumnData::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				};

				let combined_bv = match (str_bv, count_bv) {
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
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
