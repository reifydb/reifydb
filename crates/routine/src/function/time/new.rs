// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct TimeNew {
	info: RoutineInfo,
}

impl Default for TimeNew {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeNew {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::new"),
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

impl<'a> Routine<FunctionContext<'a>> for TimeNew {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 3 && args.len() != 4 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let hour_col = &args[0];
		let min_col = &args[1];
		let sec_col = &args[2];
		let nano_col = if args.len() == 4 {
			Some(&args[3])
		} else {
			None
		};

		let (hour_data, _) = hour_col.unwrap_option();
		let (min_data, _) = min_col.unwrap_option();
		let (sec_data, _) = sec_col.unwrap_option();
		let nano_data = nano_col.map(|c| c.unwrap_option());

		if !is_integer_type(hour_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
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
				actual: hour_data.get_type(),
			});
		}
		if !is_integer_type(min_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
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
				actual: min_data.get_type(),
			});
		}
		if !is_integer_type(sec_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
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
				actual: sec_data.get_type(),
			});
		}
		if let Some((nd, _)) = &nano_data
			&& !is_integer_type(nd)
		{
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
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
				actual: nd.get_type(),
			});
		}

		let row_count = hour_data.len();
		let mut container = TemporalContainer::with_capacity(row_count);

		for i in 0..row_count {
			let hour = extract_i32(hour_data, i);
			let min = extract_i32(min_data, i);
			let sec = extract_i32(sec_data, i);
			let nano = if let Some((nd, _)) = &nano_data {
				extract_i32(nd, i)
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

		let result_data = ColumnBuffer::Time(container);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
	}
}
