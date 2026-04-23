// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateNew {
	info: FunctionInfo,
}

impl Default for DateNew {
	fn default() -> Self {
		Self::new()
	}
}

impl DateNew {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("date::new"),
		}
	}
}

fn extract_i32(data: &ColumnBuffer, i: usize) -> Option<i32> {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Int4(c) => c.get(i).copied(),
		ColumnBuffer::Int8(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Int16(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as i32),
		ColumnBuffer::Uint16(c) => c.get(i).map(|&v| v as i32),
		_ => None,
	}
}

fn is_integer_type(data: &ColumnBuffer) -> bool {
	matches!(
		data,
		ColumnBuffer::Int1(_)
			| ColumnBuffer::Int2(_)
			| ColumnBuffer::Int4(_)
			| ColumnBuffer::Int8(_)
			| ColumnBuffer::Int16(_)
			| ColumnBuffer::Uint1(_)
			| ColumnBuffer::Uint2(_)
			| ColumnBuffer::Uint4(_)
			| ColumnBuffer::Uint8(_)
			| ColumnBuffer::Uint16(_)
	)
}

impl Function for DateNew {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 3 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let year_col = &args[0];
		let month_col = &args[1];
		let day_col = &args[2];
		let (year_data, _) = year_col.data().unwrap_option();
		let (month_data, _) = month_col.data().unwrap_option();
		let (day_data, _) = day_col.data().unwrap_option();
		let row_count = year_data.len();

		if !is_integer_type(year_data) {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Int16,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
					Type::Uint16,
				],
				actual: year_data.get_type(),
			});
		}
		if !is_integer_type(month_data) {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Int16,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
					Type::Uint16,
				],
				actual: month_data.get_type(),
			});
		}
		if !is_integer_type(day_data) {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Int16,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
					Type::Uint16,
				],
				actual: day_data.get_type(),
			});
		}

		let mut container = TemporalContainer::with_capacity(row_count);

		for i in 0..row_count {
			let year = extract_i32(year_data, i);
			let month = extract_i32(month_data, i);
			let day = extract_i32(day_data, i);

			match (year, month, day) {
				(Some(y), Some(m), Some(d)) => {
					if m >= 1 && d >= 1 {
						match Date::new(y, m as u32, d as u32) {
							Some(date) => container.push(date),
							None => container.push_default(),
						}
					} else {
						container.push_default();
					}
				}
				_ => container.push_default(),
			}
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::Date(container))]))
	}
}
