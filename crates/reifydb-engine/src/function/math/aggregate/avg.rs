// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::Value;

use crate::{
	columnar::ColumnData,
	function::{AggregateFunction, AggregateFunctionContext},
};

pub struct Avg {
	pub sums: HashMap<Vec<Value>, f64>,
	pub counts: HashMap<Vec<Value>, u64>,
}

impl Avg {
	pub fn new() -> Self {
		Self {
			sums: HashMap::new(),
			counts: HashMap::new(),
		}
	}
}

impl AggregateFunction for Avg {
	fn aggregate(
		&mut self,
		ctx: AggregateFunctionContext,
	) -> crate::Result<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) =
							container.get(i)
						{
							sum += *value;
							count += 1;
						}
					}

					if count > 0 {
						self.sums
							.entry(group.clone())
							.and_modify(|v| {
								*v += sum
							})
							.or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| {
								*c += count
							})
							.or_insert(count);
					}
				}
				Ok(())
			}
			ColumnData::Int2(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) =
							container.get(i)
						{
							sum += *value as f64;
							count += 1;
						}
					}

					if count > 0 {
						self.sums
							.entry(group.clone())
							.and_modify(|v| {
								*v += sum
							})
							.or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| {
								*c += count
							})
							.or_insert(count);
					}
				}
				Ok(())
			}
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data =
			ColumnData::float8_with_capacity(self.sums.len());

		for (key, sum) in std::mem::take(&mut self.sums) {
			let count = self.counts.remove(&key).unwrap_or(0);
			let avg = if count > 0 {
				sum / count as f64
			} else {
				f64::NAN // or return Value::Undefined if preferred
			};

			keys.push(key);
			data.push_value(Value::float8(avg));
		}

		Ok((keys, data))
	}
}
