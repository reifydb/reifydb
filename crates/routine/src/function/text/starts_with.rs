// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TextStartsWith {
	info: RoutineInfo,
}

impl Default for TextStartsWith {
	fn default() -> Self {
		Self::new()
	}
}

impl TextStartsWith {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::starts_with"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextStartsWith {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
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
		let prefix_col = &args[1];

		let (str_data, str_bv) = str_col.unwrap_option();
		let (prefix_data, prefix_bv) = prefix_col.unwrap_option();
		let row_count = str_data.len();

		match (str_data, prefix_data) {
			(
				ColumnBuffer::Utf8 {
					container: str_container,
					..
				},
				ColumnBuffer::Utf8 {
					container: prefix_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if str_container.is_defined(i) && prefix_container.is_defined(i) {
						let s = str_container.get(i).unwrap();
						let prefix = prefix_container.get(i).unwrap();
						result_data.push(s.starts_with(prefix));
						result_bitvec.push(true);
					} else {
						result_data.push(false);
						result_bitvec.push(false);
					}
				}

				let result_col_data = ColumnBuffer::bool_with_bitvec(result_data, result_bitvec);

				let combined_bv = match (str_bv, prefix_bv) {
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
			(
				ColumnBuffer::Utf8 {
					..
				},
				other,
			) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TextStartsWith {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
