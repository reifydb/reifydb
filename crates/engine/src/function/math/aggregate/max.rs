// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::value::columnar::ColumnData;
use reifydb_type::Value;

use crate::function::{AggregateFunction, AggregateFunctionContext};

pub struct Max {
	pub maxs: HashMap<Vec<Value>, f64>,
}

impl Max {
	pub fn new() -> Self {
		Self {
			maxs: HashMap::new(),
		}
	}
}

impl AggregateFunction for Max {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> crate::Result<()> {
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
			_ => unimplemented!(),
		}
	}

	fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnData::float8_with_capacity(self.maxs.len());

		for (key, max) in std::mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(Value::float8(max));
		}

		Ok((keys, data))
	}
}
