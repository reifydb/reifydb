// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};

use crate::{GeneratorContext, GeneratorFunction};

pub struct GenerateSeries;

impl GenerateSeries {
	pub fn new() -> Self {
		Self {}
	}
}

impl GeneratorFunction for GenerateSeries {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> crate::GeneratorFunctionResult<Columns> {
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
