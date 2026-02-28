// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{Value, r#type::Type};

use crate::{AggregateFunction, AggregateFunctionContext, error::AggregateFunctionResult};

pub struct Sum {
	pub sums: IndexMap<Vec<Value>, Value>,
}

impl Sum {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Sum {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		match &column.data() {
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let sum: f64 = indices
						.iter()
						.filter(|&i| container.is_defined(*i))
						.filter_map(|&i| container.get(i))
						.sum();

					self.sums.insert(group.clone(), Value::float8(sum));
				}
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let sum: f32 = indices
						.iter()
						.filter(|&i| container.is_defined(*i))
						.filter_map(|&i| container.get(i))
						.sum();

					self.sums.insert(group.clone(), Value::float4(sum));
				}
				Ok(())
			}
			ColumnData::Int2(container) => {
				for (group, indices) in groups.iter() {
					let sum: i16 = indices.iter().filter_map(|&i| container.get(i)).sum();

					self.sums.insert(group.clone(), Value::Int2(sum));
				}
				Ok(())
			}
			ColumnData::Int4(container) => {
				for (group, indices) in groups.iter() {
					let sum: i32 = indices
						.iter()
						.filter(|&i| container.is_defined(*i))
						.filter_map(|&i| container.get(i))
						.sum();
					self.sums.insert(group.clone(), Value::Int4(sum));
				}
				Ok(())
			}
			ColumnData::Int8(container) => {
				for (group, indices) in groups.iter() {
					let sum: i64 = indices
						.iter()
						.filter(|&i| container.is_defined(*i))
						.filter_map(|&i| container.get(i))
						.sum();

					self.sums.insert(group.clone(), Value::Int8(sum));
				}
				Ok(())
			}
			_ => unimplemented!("{}", column.get_type()),
		}
	}

	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnData::none_typed(Type::Boolean, 0);

		for (key, sum) in std::mem::take(&mut self.sums) {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data))
	}
}
