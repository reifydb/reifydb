// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let start_column = args.first().ok_or_else(|| RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let start_value = match start_column.data() {
			ColumnBuffer::Int4(container) => container.get(0).copied().unwrap_or(1),
			_ => {
				return Err(RoutineError::FunctionExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "start parameter must be an integer".to_string(),
				});
			}
		};

		let end_column = args.get(1).ok_or_else(|| RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let end_value = match end_column.data() {
			ColumnBuffer::Int4(container) => container.get(0).copied().unwrap_or(10),
			_ => {
				return Err(RoutineError::FunctionExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "end parameter must be an integer".to_string(),
				});
			}
		};

		let series: Vec<i32> = (start_value..=end_value).collect();
		let series_column = ColumnWithName::int4("value", series);

		Ok(Columns::new(vec![series_column]))
	}
}

impl Function for GenerateSeries {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Generator]
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

fn extract_i32(data: &ColumnBuffer, index: usize) -> Option<i32> {
	match data {
		ColumnBuffer::Int1(c) => c.get(index).map(|&v| v as i32),
		ColumnBuffer::Int2(c) => c.get(index).map(|&v| v as i32),
		ColumnBuffer::Int4(c) => c.get(index).copied(),
		ColumnBuffer::Int8(c) => c.get(index).map(|&v| v as i32),
		ColumnBuffer::Uint1(c) => c.get(index).map(|&v| v as i32),
		ColumnBuffer::Uint2(c) => c.get(index).map(|&v| v as i32),
		ColumnBuffer::Uint4(c) => c.get(index).map(|&v| v as i32),
		_ => None,
	}
}

impl<'a> Routine<FunctionContext<'a>> for Series {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let start_column = args.first().ok_or_else(|| RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let start_value = extract_i32(start_column.data(), 0).unwrap_or(1);

		let end_column = args.get(1).ok_or_else(|| RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let end_value = extract_i32(end_column.data(), 0).unwrap_or(10);

		let series: Vec<i32> = (start_value..=end_value).collect();
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::int4(series))]))
	}
}

impl Function for Series {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
