// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::ColumnData;

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct Max;

impl Max {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Max {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::int4(Vec::<i32>::new()));
		}

		// For max function, we need to find the maximum value across all columns for each row
		let first_column = columns.get(0).unwrap();

		match first_column.data() {
			ColumnData::Int1(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut max_value: Option<i8> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int1(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								max_value = Some(match max_value {
									None => *value,
									Some(current_max) => current_max.max(*value),
								});
							}
						}
					}

					result.push(max_value.unwrap_or(0));
				}

				Ok(ColumnData::int1(result))
			}
			ColumnData::Int2(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut max_value: Option<i16> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int2(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								max_value = Some(match max_value {
									None => *value,
									Some(current_max) => current_max.max(*value),
								});
							}
						}
					}

					result.push(max_value.unwrap_or(0));
				}

				Ok(ColumnData::int2(result))
			}
			ColumnData::Int4(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut max_value: Option<i32> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int4(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								max_value = Some(match max_value {
									None => *value,
									Some(current_max) => current_max.max(*value),
								});
							}
						}
					}

					result.push(max_value.unwrap_or(0));
				}

				Ok(ColumnData::int4(result))
			}
			ColumnData::Int8(_) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut max_value: Option<i64> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int8(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								max_value = Some(match max_value {
									None => *value,
									Some(current_max) => current_max.max(*value),
								});
							}
						}
					}

					result.push(max_value.unwrap_or(0));
				}

				Ok(ColumnData::int8(result))
			}
			_ => unimplemented!("Max function currently supports integer types only"),
		}
	}
}
