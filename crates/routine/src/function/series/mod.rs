// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct GenerateSeries {
	info: FunctionInfo,
}

impl Default for GenerateSeries {
	fn default() -> Self {
		Self::new()
	}
}

impl GenerateSeries {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("generate_series"),
		}
	}
}

impl Function for GenerateSeries {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Generator]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		// Get start value
		let start_column = args.first().ok_or_else(|| FunctionError::ArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let start_value = match start_column.data() {
			ColumnData::Int4(container) => container.get(0).copied().unwrap_or(1),
			_ => {
				return Err(FunctionError::ExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "start parameter must be an integer".to_string(),
				});
			}
		};

		// Get end value
		let end_column = args.get(1).ok_or_else(|| FunctionError::ArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let end_value = match end_column.data() {
			ColumnData::Int4(container) => container.get(0).copied().unwrap_or(10),
			_ => {
				return Err(FunctionError::ExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "end parameter must be an integer".to_string(),
				});
			}
		};

		// Generate the series
		let series: Vec<i32> = (start_value..=end_value).collect();
		let series_column = Column::int4("value", series);

		Ok(Columns::new(vec![series_column]))
	}
}

pub struct Series {
	info: FunctionInfo,
}

impl Default for Series {
	fn default() -> Self {
		Self::new()
	}
}

impl Series {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("gen::series"),
		}
	}
}

fn extract_i32(data: &ColumnData, index: usize) -> Option<i32> {
	match data {
		ColumnData::Int1(c) => c.get(index).map(|&v| v as i32),
		ColumnData::Int2(c) => c.get(index).map(|&v| v as i32),
		ColumnData::Int4(c) => c.get(index).copied(),
		ColumnData::Int8(c) => c.get(index).map(|&v| v as i32),
		ColumnData::Uint1(c) => c.get(index).map(|&v| v as i32),
		ColumnData::Uint2(c) => c.get(index).map(|&v| v as i32),
		ColumnData::Uint4(c) => c.get(index).map(|&v| v as i32),
		_ => None,
	}
}

impl Function for Series {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let start_column = args.first().ok_or_else(|| FunctionError::ArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let start_value = extract_i32(start_column.data(), 0).unwrap_or(1);

		let end_column = args.get(1).ok_or_else(|| FunctionError::ArityMismatch {
			function: ctx.fragment.clone(),
			expected: 2,
			actual: args.len(),
		})?;
		let end_value = extract_i32(end_column.data(), 0).unwrap_or(10);

		let series: Vec<i32> = (start_value..=end_value).collect();
		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), ColumnData::int4(series))]))
	}
}
