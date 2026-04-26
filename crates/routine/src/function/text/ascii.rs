// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct TextAscii {
	info: RoutineInfo,
}

impl Default for TextAscii {
	fn default() -> Self {
		Self::new()
	}
}

impl TextAscii {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::ascii"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextAscii {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
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
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let s = &container[i];
						let code_point = s.chars().next().map(|c| c as i32).unwrap_or(0);
						result_data.push(code_point);
						result_bitvec.push(true);
					} else {
						result_data.push(0);
						result_bitvec.push(false);
					}
				}

				let result_data = ColumnBuffer::int4_with_bitvec(result_data, result_bitvec);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					},
					None => result_data,
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
