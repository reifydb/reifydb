// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, ScalarFunctionError};

pub struct Avg {}

impl Avg {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Avg {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate at least 1 argument
		if columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		let mut sum = vec![0.0f64; row_count];
		let mut count = vec![0u32; row_count];

		for (col_idx, col) in columns.iter().enumerate() {
			match &col.data() {
				ColumnData::Int1(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Int2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Int4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Int8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Int16(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint1(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint16(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Float4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnData::Float8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value;
							count[i] += 1;
						}
					}
				}
				ColumnData::Int {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				ColumnData::Uint {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				ColumnData::Decimal {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				other => {
					return Err(ScalarFunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: col_idx,
						expected: vec![
							Type::Int1,
							Type::Int2,
							Type::Int4,
							Type::Int8,
							Type::Int16,
							Type::Uint1,
							Type::Uint2,
							Type::Uint4,
							Type::Uint8,
							Type::Uint16,
							Type::Float4,
							Type::Float8,
							Type::Int,
							Type::Uint,
							Type::Decimal,
						],
						actual: other.get_type(),
					});
				}
			}
		}

		let mut data = Vec::with_capacity(row_count);
		let mut valids = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if count[i] > 0 {
				data.push(sum[i] / count[i] as f64);
				valids.push(true);
			} else {
				data.push(0.0);
				valids.push(false);
			}
		}

		Ok(ColumnData::float8_with_bitvec(data, valids))
	}
}
