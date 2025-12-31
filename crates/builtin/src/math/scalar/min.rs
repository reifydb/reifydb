// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{ScalarFunction, ScalarFunctionContext},
	value::column::ColumnData,
};

pub struct Min;

impl Min {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Min {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::int4(Vec::<i32>::new()));
		}

		// For min function, we need to find the minimum value across all columns for each row
		let first_column = columns.get(0).unwrap();

		match first_column.data() {
			ColumnData::Int1(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i8> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int1(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					result.push(min_value.unwrap_or(0));
				}

				Ok(ColumnData::int1(result))
			}
			ColumnData::Int2(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i16> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int2(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					result.push(min_value.unwrap_or(0));
				}

				Ok(ColumnData::int2(result))
			}
			ColumnData::Int4(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i32> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int4(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					result.push(min_value.unwrap_or(0));
				}

				Ok(ColumnData::int4(result))
			}
			ColumnData::Int8(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i64> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int8(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					result.push(min_value.unwrap_or(0));
				}

				Ok(ColumnData::int8(result))
			}
			_ => unimplemented!("Min function currently supports integer types only"),
		}
	}
}
