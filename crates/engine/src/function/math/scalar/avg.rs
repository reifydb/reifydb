// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::columnar::ColumnData;

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct Avg {}

impl Avg {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Avg {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let mut sum = vec![0.0f64; row_count];
		let mut count = vec![0u32; row_count];

		for col in columns.iter() {
			match &col.data() {
				ColumnData::Int2(container) => {
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
				data => unimplemented!("{data:?}"),
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
