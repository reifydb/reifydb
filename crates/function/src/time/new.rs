// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct TimeNew;

impl TimeNew {
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

impl ScalarFunction for TimeNew {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 3 && columns.len() != 4 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: columns.len(),
			});
		}

		let hour_col = columns.get(0).unwrap();
		let min_col = columns.get(1).unwrap();
		let sec_col = columns.get(2).unwrap();
		let nano_col = if columns.len() == 4 {
			Some(columns.get(3).unwrap())
		} else {
			None
		};

		if !is_integer_type(hour_col.data()) {
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
				actual: hour_col.data().get_type(),
			});
		}
		if !is_integer_type(min_col.data()) {
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
				actual: min_col.data().get_type(),
			});
		}
		if !is_integer_type(sec_col.data()) {
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
				actual: sec_col.data().get_type(),
			});
		}
		if let Some(nc) = &nano_col {
			if !is_integer_type(nc.data()) {
				return Err(ScalarFunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 3,
					expected: vec![
						Type::Int1,
						Type::Int2,
						Type::Int4,
						Type::Int8,
						Type::Uint1,
						Type::Uint2,
						Type::Uint4,
					],
					actual: nc.data().get_type(),
				});
			}
		}

		let mut container = TemporalContainer::with_capacity(row_count);

		for i in 0..row_count {
			let hour = extract_i32(hour_col.data(), i);
			let min = extract_i32(min_col.data(), i);
			let sec = extract_i32(sec_col.data(), i);
			let nano = if let Some(nc) = &nano_col {
				extract_i32(nc.data(), i)
			} else {
				Some(0)
			};

			match (hour, min, sec, nano) {
				(Some(h), Some(m), Some(s), Some(n)) => {
					if h >= 0 && m >= 0 && s >= 0 && n >= 0 {
						match Time::new(h as u32, m as u32, s as u32, n as u32) {
							Some(time) => container.push(time),
							None => container.push_default(),
						}
					} else {
						container.push_default();
					}
				}
				_ => container.push_default(),
			}
		}

		Ok(ColumnData::Time(container))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}
}
