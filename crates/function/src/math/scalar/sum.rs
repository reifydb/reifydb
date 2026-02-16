// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext, propagate_options};

pub struct Sum {}

impl Sum {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Sum {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let mut sum = vec![0.0f64; row_count];
		let mut has_values = vec![false; row_count];

		for col in columns.iter() {
			match &col.data() {
				ColumnData::Int2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							has_values[i] = true;
						}
					}
				}
				ColumnData::Int4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							has_values[i] = true;
						}
					}
				}
				ColumnData::Int8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							has_values[i] = true;
						}
					}
				}
				ColumnData::Float4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							has_values[i] = true;
						}
					}
				}
				ColumnData::Float8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value;
							has_values[i] = true;
						}
					}
				}
				data => unimplemented!("{data:?}"),
			}
		}

		let mut data = Vec::with_capacity(row_count);
		let mut valids = Vec::with_capacity(row_count);

		for i in 0..row_count {
			data.push(sum[i]);
			valids.push(has_values[i]);
		}

		Ok(ColumnData::float8_with_bitvec(data, valids))
	}
}
