// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TextRepeat {
	info: RoutineInfo,
}

impl Default for TextRepeat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextRepeat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::repeat"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextRepeat {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let str_col = &args[0];
		let count_col = &args[1];

		let (str_data, str_bv) = str_col.unwrap_option();
		let (count_data, count_bv) = count_col.unwrap_option();
		let row_count = str_data.len();

		match str_data {
			ColumnBuffer::Utf8 {
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
						ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Int8(c) => c.get(i).copied(),
						ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i64),
						ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i64),
						_ => {
							return Err(RoutineError::FunctionInvalidArgumentType {
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
							let s = str_container.get(i).unwrap();
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

				let result_col_data = ColumnBuffer::Utf8 {
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
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv,
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextRepeat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
