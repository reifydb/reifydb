// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, LargeStringArray};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{constraint::bytes::MaxBytes, value_type::ValueType};

pub struct TextConcat {
	info: RoutineInfo,
}

impl Default for TextConcat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextConcat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::concat"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextConcat {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let row_count = args[0].len();

		for (idx, col) in args.iter().enumerate() {
			match col.data() {
				ColumnBuffer::Utf8 {
					..
				} => {}
				other => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: idx,
						expected: vec![ValueType::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let mut result_data = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let mut all_defined = true;
			let mut concatenated = String::new();

			for col in args.iter() {
				if let ColumnBuffer::Utf8 {
					container,
					..
				} = col.data()
				{
					if i < container.len() {
						concatenated.push_str(container.value(i));
					} else {
						all_defined = false;
						break;
					}
				}
			}

			if all_defined {
				result_data.push(concatenated);
			} else {
				result_data.push(String::new());
			}
		}

		let result_col_data = ColumnBuffer::Utf8 {
			container: LargeStringArray::from(result_data),
			max_bytes: MaxBytes::MAX,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_col_data)]))
	}
}

impl Function for TextConcat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::AtLeast(2)
	}
}
