// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, LargeStringArray};
use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{constraint::bytes::MaxBytes, value_type::ValueType};

pub struct TextReplace {
	info: RoutineInfo,
}

impl Default for TextReplace {
	fn default() -> Self {
		Self::new()
	}
}

impl TextReplace {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::replace"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextReplace {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let str_col = &args[0];
		let from_col = &args[1];
		let to_col = &args[2];

		let (str_data, str_bv) = str_col.unwrap_option();
		let (from_data, from_bv) = from_col.unwrap_option();
		let (to_data, to_bv) = to_col.unwrap_option();
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
					if i < str_container.len() && i < from_container.len() && i < to_container.len()
					{
						let s = str_container.value(i);
						let from = from_container.value(i);
						let to = to_container.value(i);
						result_data.push(s.replace(from, to));
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: LargeStringArray::from(result_data),
					max_bytes: MaxBytes::MAX,
				};

				let mut combined_bv: Option<BooleanBuffer> = None;
				for bv in [str_bv, from_bv, to_bv].into_iter().flatten() {
					combined_bv = Some(match combined_bv {
						Some(existing) => &existing & bv,
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
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
			(
				ColumnBuffer::Utf8 {
					..
				},
				other,
				_,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
			(other, _, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextReplace {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
