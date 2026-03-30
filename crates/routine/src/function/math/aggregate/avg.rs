// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{
	Value,
	r#type::{Type, input_types::InputTypes},
};

use crate::function::{
	AggregateFunction, AggregateFunctionContext,
	error::{AggregateFunctionError, AggregateFunctionResult},
};

pub struct Avg {
	pub sums: IndexMap<Vec<Value>, f64>,
	pub counts: IndexMap<Vec<Value>, u64>,
}

impl Default for Avg {
	fn default() -> Self {
		Self::new()
	}
}

impl Avg {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
			counts: IndexMap::new(),
		}
	}
}

macro_rules! avg_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr) => {
		for (group, indices) in $groups.iter() {
			let mut sum = 0.0f64;
			let mut count = 0u64;
			for &i in indices {
				if $column.data().is_defined(i) {
					if let Some(&val) = $container.get(i) {
						sum += val as f64;
						count += 1;
					}
				}
			}
			if count > 0 {
				$self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
				$self.counts.entry(group.clone()).and_modify(|c| *c += count).or_insert(count);
			} else {
				$self.sums.entry(group.clone()).or_insert(0.0);
				$self.counts.entry(group.clone()).or_insert(0);
			}
		}
	};
}

impl AggregateFunction for Avg {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;
		let (data, _bitvec) = column.data().unwrap_option();

		match data {
			ColumnData::Int1(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Int2(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Int4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Int8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Int16(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Uint1(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Uint2(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Uint4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Uint8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Uint16(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Float4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Float8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnData::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			ColumnData::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			other => Err(AggregateFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: self.accepted_types().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<Vec<Value>>, ColumnData)> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnData::float8_with_capacity(self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			let count = self.counts.swap_remove(&key).unwrap_or(0);
			keys.push(key);
			if count > 0 {
				data.push_value(Value::float8(sum / count as f64));
			} else {
				data.push_value(Value::none());
			}
		}

		Ok((keys, data))
	}

	fn return_type(&self, _input_type: &Type) -> Type {
		Type::Float8
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}
}
