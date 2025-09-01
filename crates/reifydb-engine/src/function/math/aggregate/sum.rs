// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::value::columnar::ColumnData;
use reifydb_type::Value;

use crate::function::{AggregateFunction, AggregateFunctionContext};

pub struct Sum {
	pub sums: HashMap<Vec<Value>, Value>,
}

impl Sum {
	pub fn new() -> Self {
		Self {
			sums: HashMap::new(),
		}
	}
}

impl AggregateFunction for Sum {
	fn aggregate(
		&mut self,
		ctx: AggregateFunctionContext,
	) -> crate::Result<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let sum: f64 = indices
						.iter()
						.filter_map(|&i| {
							container.get(i)
						})
						.sum();

					self.sums.insert(
						group.clone(),
						Value::float8(sum),
					);
				}
				Ok(())
			}
			ColumnData::Int4(container) => {
				for (group, indices) in groups.iter() {
					let sum: i32 = indices
						.iter()
						.filter_map(|&i| {
							container.get(i)
						})
						.sum();

					self.sums.insert(
						group.clone(),
						Value::Int4(sum),
					);
				}
				Ok(())
			}
			ColumnData::Int8(container) => {
				for (group, indices) in groups.iter() {
					let sum: i64 = indices
						.iter()
						.filter_map(|&i| {
							container.get(i)
						})
						.sum();

					self.sums.insert(
						group.clone(),
						Value::Int8(sum),
					);
				}
				Ok(())
			}
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnData::undefined(0);

		for (key, sum) in std::mem::take(&mut self.sums) {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data))
	}
}
