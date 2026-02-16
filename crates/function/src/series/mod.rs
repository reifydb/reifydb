// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};

use crate::{GeneratorContext, GeneratorFunction, ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct GenerateSeries;

impl GenerateSeries {
	pub fn new() -> Self {
		Self {}
	}
}

impl GeneratorFunction for GenerateSeries {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> crate::error::GeneratorFunctionResult<Columns> {
		// Extract parameters: start and end
		let params = &ctx.params;

		assert_eq!(params.len(), 2, "generate_series requires exactly 2 parameters: start and end");

		// Get start value
		let start_column = params.get(0).unwrap();
		let start_value = match start_column.data() {
			ColumnData::Int4(container) => container.get(0).copied().unwrap_or(1),
			_ => panic!("start parameter must be an integer"),
		};

		// Get end value
		let end_column = params.get(1).unwrap();
		let end_value = match end_column.data() {
			ColumnData::Int4(container) => container.get(0).copied().unwrap_or(10),
			_ => panic!("end parameter must be an integer"),
		};

		// Generate the series
		let series: Vec<i32> = (start_value..=end_value).collect();
		let series_column = Column::int4("value", series);

		Ok(Columns::new(vec![series_column]))
	}
}

pub struct Series;

impl Series {
	pub fn new() -> Self {
		Self {}
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

impl ScalarFunction for Series {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let start_column = columns.get(0).unwrap();
		let start_value = extract_i32(start_column.data(), 0).unwrap_or(1);

		let end_column = columns.get(1).unwrap();
		let end_value = extract_i32(end_column.data(), 0).unwrap_or(10);

		let series: Vec<i32> = (start_value..=end_value).collect();
		Ok(ColumnData::int4(series))
	}
}
