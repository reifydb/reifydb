// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use crate::function::support::coerce::read_i32;
use reifydb_value::value::{
	container::temporal_array::time_array,
	time::Time,
	value_type::ValueType,
};

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let hour_data = &args[0];
		let min_data = &args[1];
		let sec_data = &args[2];
		let nano_data = if args.len() == 4 {
			Some(&args[3])
		} else {
			None
		};

		if !is_integer_type(hour_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: hour_data.get_type(),
			});
		}
		if !is_integer_type(min_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: min_data.get_type(),
			});
		}
		if !is_integer_type(sec_data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 2,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: sec_data.get_type(),
			});
		}
		if let Some(nd) = nano_data
			&& !is_integer_type(nd)
		{
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 3,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: nd.get_type(),
			});
		}

		let row_count = hour_data.len();
		let mut container = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let hour = read_i32(&ctx.fragment, hour_data, i)?;
			let min = read_i32(&ctx.fragment, min_data, i)?;
			let sec = read_i32(&ctx.fragment, sec_data, i)?;
			let nano = if let Some(nd) = nano_data {
				read_i32(&ctx.fragment, nd, i)?
			} else {
				Some(0)
			};

			match (hour, min, sec, nano) {
				(Some(h), Some(m), Some(s), Some(n)) => {
					let time = u32::try_from(h)
						.ok()
						.zip(u32::try_from(m).ok())
						.zip(u32::try_from(s).ok())
						.zip(u32::try_from(n).ok())
						.and_then(|(((h, m), s), n)| Time::new(h, m, s, n))
						.ok_or_else(|| {
							failed(ctx, format!("{h}:{m}:{s}.{n} is not a time of day"))
						})?;
					container.push(time);
				}
				_ => container.push(Time::default()),
			}
		}

		let result_data = ColumnBuffer::Time(time_array(container));
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for TimeNew {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Range(3, 4)
	}
}
