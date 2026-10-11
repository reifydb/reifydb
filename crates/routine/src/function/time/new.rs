// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::time_array,
	time::Time,
	value_type::ValueType,
};

use crate::function::support::{coerce::read_i32, column::array_column};

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

fn is_integer_type(data: &ColumnView) -> bool {
	matches!(
		data.data,
		ViewData::Int1(_)
			| ViewData::Int2(_)
			| ViewData::Int4(_)
			| ViewData::Int8(_)
			| ViewData::Int16(_)
			| ViewData::Uint1(_)
			| ViewData::Uint2(_)
			| ViewData::Uint4(_)
			| ViewData::Uint8(_)
			| ViewData::Uint16(_)
	)
}

impl<'a> Routine<FunctionContext<'a>> for TimeNew {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let hour_data = ColumnView::try_from(&args[0])?;
		let min_data = ColumnView::try_from(&args[1])?;
		let sec_data = ColumnView::try_from(&args[2])?;
		let nano_data = if args.len() == 4 {
			Some(ColumnView::try_from(&args[3])?)
		} else {
			None
		};

		if !is_integer_type(&hour_data) {
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
		if !is_integer_type(&min_data) {
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
		if !is_integer_type(&sec_data) {
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
		if let Some(nd) = &nano_data
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
			let hour = read_i32(&ctx.fragment, &hour_data, i)?;
			let min = read_i32(&ctx.fragment, &min_data, i)?;
			let sec = read_i32(&ctx.fragment, &sec_data, i)?;
			let nano = if let Some(nd) = &nano_data {
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

		Ok(array_column(ctx.fragment.text(), ValueType::Time, Arc::new(time_array(container))))
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
