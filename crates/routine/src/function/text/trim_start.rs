// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::utf8::Utf8Container, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct TextTrimStart {
	info: RoutineInfo,
}

impl Default for TextTrimStart {
	fn default() -> Self {
		Self::new()
	}
}

impl TextTrimStart {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::trim_start"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextTrimStart {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let original_str = &container[i];
						let trimmed_str = original_str.trim_start();
						result_data.push(trimmed_str.to_string());
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: *max_bytes,
				};
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv.clone(),
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
