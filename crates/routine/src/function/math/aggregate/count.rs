// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{
	Value,
	r#type::{Type, input_types::InputTypes},
};

use crate::function::{AggregateFunction, AggregateFunctionContext, error::AggregateFunctionResult};

pub struct Count {
	pub counts: IndexMap<Vec<Value>, i64>,
}

impl Default for Count {
	fn default() -> Self {
		Self::new()
	}
}

impl Count {
	pub fn new() -> Self {
		Self {
			counts: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Count {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;

		// Check if this is count(*) by examining if we have a dummy column
		let is_count_star = column.name.text() == "dummy" && matches!(column.data(), ColumnData::Int4(_));

		if is_count_star {
			// For count(*), count all rows including those with undefined values
			for (group, indices) in groups.iter() {
				let count = indices.len() as i64;
				self.counts.insert(group.clone(), count);
			}
		} else {
			// For count(column), only count defined (non-null) values
			// is_defined handles both plain and Option-wrapped columns
			for (group, indices) in groups.iter() {
				let count = indices.iter().filter(|&i| column.data().is_defined(*i)).count() as i64;
				self.counts.insert(group.clone(), count);
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.counts.len());
		let mut data = ColumnData::int8_with_capacity(self.counts.len());

		for (key, count) in mem::take(&mut self.counts) {
			keys.push(key);
			data.push_value(Value::Int8(count));
		}

		Ok((keys, data))
	}

	fn return_type(&self, _input_type: &Type) -> Type {
		Type::Int8
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}
}
