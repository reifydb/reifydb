// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_core::value::column::ColumnData;
use reifydb_type::Value;

use crate::{AggregateFunction, AggregateFunctionContext};

pub struct Count {
	pub counts: IndexMap<Vec<Value>, i64>,
}

impl Count {
	pub fn new() -> Self {
		Self {
			counts: IndexMap::new(),
		}
	}
}

impl AggregateFunction for Count {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> crate::Result<()> {
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
			match &column.data() {
				ColumnData::Bool(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Float8(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Float4(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Int4(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Int8(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Int2(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Int1(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Int16(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Utf8 {
					container,
					..
				} => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Date(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::DateTime(container) => {
					for (group, indices) in groups.iter() {
						let count = indices.iter().filter(|&i| container.is_defined(*i)).count()
							as i64;
						self.counts.insert(group.clone(), count);
					}
				}
				ColumnData::Undefined(_) => {
					// Undefined columns have no defined values to count
					for (group, _indices) in groups.iter() {
						self.counts.insert(group.clone(), 0);
					}
				}
				_ => {
					// For other column types, use generic is_defined check
					for (group, indices) in groups.iter() {
						let count = indices
							.iter()
							.filter(|&i| column.data().is_defined(*i))
							.count() as i64;
						self.counts.insert(group.clone(), count);
					}
				}
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> crate::Result<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.counts.len());
		let mut data = ColumnData::int8_with_capacity(self.counts.len());

		for (key, count) in std::mem::take(&mut self.counts) {
			keys.push(key);
			data.push_value(Value::Int8(count));
		}

		Ok((keys, data))
	}
}
