// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::value::column::ColumnData;
use reifydb_type::Value;

use crate::function::{AggregateFunction, AggregateFunctionContext};

pub struct Count {
	pub counts: HashMap<Vec<Value>, i64>,
}

impl Count {
	pub fn new() -> Self {
		Self {
			counts: HashMap::new(),
		}
	}
}

impl AggregateFunction for Count {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> crate::Result<()> {
		let groups = &ctx.groups;

		eprintln!("DEBUG Count::aggregate: {} groups", groups.len());
		for (i, (group, indices)) in groups.iter().enumerate() {
			let count = indices.len() as i64;
			eprintln!(
				"DEBUG Count::aggregate: Group {}: {:?} with {} indices: {:?}",
				i, group, count, indices
			);
			self.counts.insert(group.clone(), count);
		}
		Ok(())
	}

	fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.counts.len());
		let mut data = ColumnData::int8_with_capacity(self.counts.len());

		eprintln!("DEBUG Count::finalize: {} counts", self.counts.len());
		for (key, count) in std::mem::take(&mut self.counts) {
			eprintln!("DEBUG Count::finalize: Key {:?} -> Count {}", key, count);
			keys.push(key);
			data.push_value(Value::Int8(count));
		}

		Ok((keys, data))
	}
}
