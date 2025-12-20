// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::ColumnData;

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct Sum {}

impl Sum {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Sum {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
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
