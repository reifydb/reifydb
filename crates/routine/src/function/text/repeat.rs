// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::bytes::MaxBytes,
	value_type::ValueType,
};

use crate::function::support::column::utf8_column;

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let str_data = ColumnView::try_from(&args[0])?;
		let count_data = ColumnView::try_from(&args[1])?;

		let row_count = str_data.len();

		match &str_data.data {
			ViewData::Utf8 {
				container: str_container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if i >= str_container.len() {
						result_data.push(String::new());
						continue;
					}

					let count = match &count_data.data {
						ViewData::Int1(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int2(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int4(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Int8(c) => c.values().get(i).copied(),
						ViewData::Uint1(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Uint2(c) => c.values().get(i).map(|&v| v as i64),
						ViewData::Uint4(c) => c.values().get(i).map(|&v| v as i64),
						_ => {
							return Err(RoutineError::FunctionInvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									ValueType::Int1,
									ValueType::Int2,
									ValueType::Int4,
									ValueType::Int8,
								],
								actual: count_data.get_type(),
							});
						}
					};

					match count {
						Some(n) if n >= 0 => {
							let s = str_container.value(i);
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

				Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, LargeStringArray::from(result_data)))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: str_data.get_type(),
			}),
		}
	}
}

impl Function for TextRepeat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
