// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct TextRepeat;

impl TextRepeat {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextRepeat {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let str_col = columns.get(0).unwrap();
		let count_col = columns.get(1).unwrap();

		match str_col.data() {
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

					let count = match count_col.data() {
						ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Int8(c) => c.get(i).copied(),
						ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
						ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
						_ => {
							return Err(ScalarFunctionError::InvalidArgumentType {
								function: ctx.fragment.clone(),
								argument_index: 1,
								expected: vec![
									Type::Int1,
									Type::Int2,
									Type::Int4,
									Type::Int8,
								],
								actual: count_col.data().get_type(),
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

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				})
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
