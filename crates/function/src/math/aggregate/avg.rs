// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::Value;

use crate::{AggregateFunction, AggregateFunctionContext, error::AggregateFunctionResult};

pub struct Avg {
	pub sums: IndexMap<Vec<Value>, f64>,
	pub counts: IndexMap<Vec<Value>, u64>,
}

impl Avg {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
			counts: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Avg {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) = container.get(i) {
							sum += *value;
							count += 1;
						}
					}

					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					}
				}
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) = container.get(i) {
							sum += *value as f64;
							count += 1;
						}
					}

					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
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
						if let Some(value) = container.get(i) {
							sum += *value as f64;
							count += 1;
						}
					}

					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					}
				}
				Ok(())
			}
			ColumnData::Int4(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) = container.get(i) {
							sum += *value as f64;
							count += 1;
						}
					}

					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					}
				}
				Ok(())
			}
			ColumnData::Int8(container) => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0;
					let mut count = 0;

					for &i in indices {
						if let Some(value) = container.get(i) {
							sum += *value as f64;
							count += 1;
						}
					}

					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);

						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					}
				}
				Ok(())
			}
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnData::float8_with_capacity(self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			let count = self.counts.swap_remove(&key).unwrap_or(0);
			let avg = if count > 0 {
				sum / count as f64
			} else {
				keys.push(key);
				data.push_value(Value::none());
				return Ok((keys, data));
			};

			keys.push(key);
			data.push_value(Value::float8(avg));
		}

		Ok((keys, data))
	}
}
