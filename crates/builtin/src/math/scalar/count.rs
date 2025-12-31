// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::ColumnData;

use reifydb_core::interface::{ScalarFunction, ScalarFunctionContext};

pub struct Count {}

impl Count {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Count {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let mut count = vec![0u32; row_count];

		for col in columns.iter() {
			match &col.data() {
				ColumnData::Int2(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				ColumnData::Int4(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				ColumnData::Int8(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				ColumnData::Float4(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				ColumnData::Float8(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				ColumnData::Bool(container) => {
					for i in 0..row_count {
						if container.get(i).is_some() {
							count[i] += 1;
						}
					}
				}
				data => unimplemented!("{data:?}"),
			}
		}

		// Convert count to f64 for consistency with other aggregation functions
		let mut data = Vec::with_capacity(row_count);
		let mut valids = Vec::with_capacity(row_count);

		for i in 0..row_count {
			data.push(count[i] as f64);
			valids.push(true); // Count is always valid (0 if no values)
		}

		Ok(ColumnData::float8_with_bitvec(data, valids))
	}
}
