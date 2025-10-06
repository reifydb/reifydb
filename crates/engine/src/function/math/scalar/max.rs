// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::ColumnData;

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct Max {}

impl Max {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Max {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let mut max_values = vec![None::<f64>; row_count];

		for col in columns.iter() {
			match &col.data() {
				ColumnData::Int2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							let val = *value as f64;
							max_values[i] =
								Some(max_values[i].map_or(val, |curr| curr.max(val)));
						}
					}
				}
				ColumnData::Int4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							let val = *value as f64;
							max_values[i] =
								Some(max_values[i].map_or(val, |curr| curr.max(val)));
						}
					}
				}
				ColumnData::Int8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							let val = *value as f64;
							max_values[i] =
								Some(max_values[i].map_or(val, |curr| curr.max(val)));
						}
					}
				}
				ColumnData::Float4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							let val = *value as f64;
							max_values[i] =
								Some(max_values[i].map_or(val, |curr| curr.max(val)));
						}
					}
				}
				ColumnData::Float8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							let val = *value;
							max_values[i] =
								Some(max_values[i].map_or(val, |curr| curr.max(val)));
						}
					}
				}
				data => unimplemented!("{data:?}"),
			}
		}

		let mut data = Vec::with_capacity(row_count);
		let mut valids = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if let Some(max_val) = max_values[i] {
				data.push(max_val);
				valids.push(true);
			} else {
				data.push(0.0);
				valids.push(false);
			}
		}

		Ok(ColumnData::float8_with_bitvec(data, valids))
	}
}
