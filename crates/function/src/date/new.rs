// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateNew;

impl DateNew {
	pub fn new() -> Self {
		Self
	}
}

fn extract_i32(data: &ColumnData, i: usize) -> Option<i32> {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Int4(c) => c.get(i).copied(),
		ColumnData::Int8(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as i32),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as i32),
		_ => None,
	}
}

fn is_integer_type(data: &ColumnData) -> bool {
	matches!(
		data,
		ColumnData::Int1(_)
			| ColumnData::Int2(_) | ColumnData::Int4(_)
			| ColumnData::Int8(_) | ColumnData::Int16(_)
			| ColumnData::Uint1(_)
			| ColumnData::Uint2(_)
			| ColumnData::Uint4(_)
			| ColumnData::Uint8(_)
			| ColumnData::Uint16(_)
	)
}

impl ScalarFunction for DateNew {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 3 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: columns.len(),
			});
		}

		let year_col = columns.get(0).unwrap();
		let month_col = columns.get(1).unwrap();
		let day_col = columns.get(2).unwrap();

		if !is_integer_type(year_col.data()) {
			return Err(ScalarFunctionError::InvalidArgumentType {
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
				actual: year_col.data().get_type(),
			});
		}
		if !is_integer_type(month_col.data()) {
			return Err(ScalarFunctionError::InvalidArgumentType {
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
				actual: month_col.data().get_type(),
			});
		}
		if !is_integer_type(day_col.data()) {
			return Err(ScalarFunctionError::InvalidArgumentType {
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
				actual: day_col.data().get_type(),
			});
		}

		let mut container = TemporalContainer::with_capacity(row_count);

		for i in 0..row_count {
			let year = extract_i32(year_col.data(), i);
			let month = extract_i32(month_col.data(), i);
			let day = extract_i32(day_col.data(), i);

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

		Ok(ColumnData::Date(container))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}
}
