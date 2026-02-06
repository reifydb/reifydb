// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::Value;

use crate::{AggregateFunction, AggregateFunctionContext};

pub struct Min {
	pub mins: IndexMap<Vec<Value>, f64>,
}

impl Min {
	pub fn new() -> Self {
		Self {
			mins: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Min {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> crate::error::AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let min_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.min_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(min_val) = min_val {
						self.mins
							.entry(group.clone())
							.and_modify(|v| *v = f64::min(*v, *min_val))
							.or_insert(*min_val);
					}
				}
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let min_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.min_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(min_val) = min_val {
						self.mins
							.entry(group.clone())
							.and_modify(|v| *v = f64::min(*v, *min_val as f64))
							.or_insert(*min_val as f64);
					}
				}
				Ok(())
			}
			ColumnData::Int2(container) => {
				for (group, indices) in groups.iter() {
					let min_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.min_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(min_val) = min_val {
						self.mins
							.entry(group.clone())
							.and_modify(|v| *v = f64::min(*v, *min_val as f64))
							.or_insert(*min_val as f64);
					}
				}
				Ok(())
			}
			ColumnData::Int4(container) => {
				for (group, indices) in groups.iter() {
					let min_val = indices
						.iter()
						.filter_map(|&i| container.get(i))
						.min_by(|a, b| a.partial_cmp(b).unwrap());

					if let Some(min_val) = min_val {
						self.mins
							.entry(group.clone())
							.and_modify(|v| *v = f64::min(*v, *min_val as f64))
							.or_insert(*min_val as f64);
					}
				}
				Ok(())
			}
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> crate::error::AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.mins.len());
		let mut data = ColumnData::float8_with_capacity(self.mins.len());

		for (key, min) in std::mem::take(&mut self.mins) {
			keys.push(key);
			data.push_value(Value::float8(min));
		}

		Ok((keys, data))
	}
}
