// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

use crate::function::support::coerce::read_i32;

pub struct GenerateSeries {
	info: RoutineInfo,
}

impl Default for GenerateSeries {
	fn default() -> Self {
		Self::new()
	}
}

impl GenerateSeries {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("series::generate"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for GenerateSeries {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let start = ColumnView::try_from(&args[0])?;
		let start_value = match &start.data {
			ViewData::Int4(container) => container.values().first().copied().unwrap_or(1),
			_ => {
				return Err(RoutineError::FunctionExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "start parameter must be an integer".to_string(),
				});
			}
		};

		let end = ColumnView::try_from(&args[1])?;
		let end_value = match &end.data {
			ViewData::Int4(container) => container.values().first().copied().unwrap_or(10),
			_ => {
				return Err(RoutineError::FunctionExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "end parameter must be an integer".to_string(),
				});
			}
		};

		let series: Vec<i32> = (start_value..=end_value).collect();
		Ok(int4("value", series))
	}
}

impl Function for GenerateSeries {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Generator]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}

pub struct Series {
	info: RoutineInfo,
}

impl Default for Series {
	fn default() -> Self {
		Self::new()
	}
}

impl Series {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("gen::series"),
		}
	}
}

fn bound(ctx: &FunctionContext, data: &ColumnView, argument_index: usize) -> Result<i32, RoutineError> {
	read_i32(&ctx.fragment, data, 0)?.ok_or_else(|| RoutineError::FunctionInvalidArgumentType {
		function: ctx.fragment.clone(),
		argument_index,
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
		actual: data.get_type(),
	})
}

impl<'a> Routine<FunctionContext<'a>> for Series {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let start_value = bound(ctx, &ColumnView::try_from(&args[0])?, 0)?;
		let end_value = bound(ctx, &ColumnView::try_from(&args[1])?, 1)?;

		let series: Vec<i32> = (start_value..=end_value).collect();
		Ok(int4(ctx.fragment.text(), series))
	}
}

impl Function for Series {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
