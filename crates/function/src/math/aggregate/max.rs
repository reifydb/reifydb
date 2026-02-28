// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::Value;

use crate::{AggregateFunction, AggregateFunctionContext, error::AggregateFunctionResult};

pub struct Max {
	pub maxs: IndexMap<Vec<Value>, f64>,
}

impl Max {
	pub fn new() -> Self {
		Self {
			maxs: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Max {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let max_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.max_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(max_val) = max_val {
						self.maxs
							.entry(group.clone())
							.and_modify(|v| *v = f64::max(*v, *max_val))
							.or_insert(*max_val);
					}
				}
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let max_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.max_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(max_val) = max_val {
						self.maxs
							.entry(group.clone())
							.and_modify(|v| *v = f64::max(*v, *max_val as f64))
							.or_insert(*max_val as f64);
					}
				}
				Ok(())
			}
			ColumnData::Int4(container) => {
				for (group, indices) in groups.iter() {
					let max_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.max_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(max_val) = max_val {
						self.maxs
							.entry(group.clone())
							.and_modify(|v| *v = f64::max(*v, *max_val as f64))
							.or_insert(*max_val as f64);
					}
				}
				Ok(())
			}
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnData::float8_with_capacity(self.maxs.len());

		for (key, max) in std::mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(Value::float8(max));
		}

		Ok((keys, data))
	}
}
