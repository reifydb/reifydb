// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct Round;

impl Default for Round {
	fn default() -> Self {
		Self {}
	}
}

impl Round {
	pub fn new() -> Self {
		Self::default()
	}
}

impl ScalarFunction for Round {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let value_column = columns.first().unwrap();

		// Get precision column if provided (default to 0)
		let precision_column = columns.get(1);

		match value_column.data() {
			ColumnData::Float4(container) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						let precision = if let Some(prec_col) = precision_column {
							match prec_col.data() {
								ColumnData::Int4(prec_container) => prec_container
									.get(row_idx)
									.copied()
									.unwrap_or(0),
								ColumnData::Int1(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								ColumnData::Int2(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								ColumnData::Int8(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								_ => 0,
							}
						} else {
							0
						};

						let multiplier = 10_f32.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
					} else {
						result.push(0.0);
					}
				}

				Ok(ColumnData::float4(result))
			}
			ColumnData::Float8(container) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						let precision = if let Some(prec_col) = precision_column {
							match prec_col.data() {
								ColumnData::Int4(prec_container) => prec_container
									.get(row_idx)
									.copied()
									.unwrap_or(0),
								ColumnData::Int1(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								ColumnData::Int2(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								ColumnData::Int8(prec_container) => prec_container
									.get(row_idx)
									.map(|&v| v as i32)
									.unwrap_or(0),
								_ => 0,
							}
						} else {
							0
						};

						let multiplier = 10_f64.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
					} else {
						result.push(0.0);
					}
				}

				Ok(ColumnData::float8(result))
			}
			ColumnData::Int4(container) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
					} else {
						result.push(0);
					}
				}

				Ok(ColumnData::int4(result))
			}
			_ => unimplemented!("Round function currently supports float4, float8, and int4 types"),
		}
	}
}
