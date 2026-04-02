// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{
	Value,
	decimal::Decimal,
	int::Int,
	r#type::{Type, input_types::InputTypes},
	uint::Uint,
};

use crate::function::{
	AggregateFunction, AggregateFunctionContext,
	error::{AggregateFunctionError, AggregateFunctionResult},
};

pub struct Sum {
	pub sums: IndexMap<Vec<Value>, Value>,
	input_type: Option<Type>,
}

impl Default for Sum {
	fn default() -> Self {
		Self::new()
	}
}

impl Sum {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
			input_type: None,
		}
	}
}

macro_rules! sum_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $ctor:expr) => {
		for (group, indices) in $groups.iter() {
			let mut sum: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.data().is_defined(i) {
					if let Some(&val) = $container.get(i) {
						sum += val;
						has_value = true;
					}
				}
			}
			if has_value {
				$self.sums.insert(group.clone(), $ctor(sum));
			} else {
				$self.sums.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

impl AggregateFunction for Sum {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;
		let (data, _bitvec) = column.data().unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnData::Int1(container) => {
				sum_arm!(self, column, groups, container, i8, Value::Int1);
				Ok(())
			}
			ColumnData::Int2(container) => {
				sum_arm!(self, column, groups, container, i16, Value::Int2);
				Ok(())
			}
			ColumnData::Int4(container) => {
				sum_arm!(self, column, groups, container, i32, Value::Int4);
				Ok(())
			}
			ColumnData::Int8(container) => {
				sum_arm!(self, column, groups, container, i64, Value::Int8);
				Ok(())
			}
			ColumnData::Int16(container) => {
				sum_arm!(self, column, groups, container, i128, Value::Int16);
				Ok(())
			}
			ColumnData::Uint1(container) => {
				sum_arm!(self, column, groups, container, u8, Value::Uint1);
				Ok(())
			}
			ColumnData::Uint2(container) => {
				sum_arm!(self, column, groups, container, u16, Value::Uint2);
				Ok(())
			}
			ColumnData::Uint4(container) => {
				sum_arm!(self, column, groups, container, u32, Value::Uint4);
				Ok(())
			}
			ColumnData::Uint8(container) => {
				sum_arm!(self, column, groups, container, u64, Value::Uint8);
				Ok(())
			}
			ColumnData::Uint16(container) => {
				sum_arm!(self, column, groups, container, u128, Value::Uint16);
				Ok(())
			}
			ColumnData::Float4(container) => {
				sum_arm!(self, column, groups, container, f32, Value::float4);
				Ok(())
			}
			ColumnData::Float8(container) => {
				sum_arm!(self, column, groups, container, f64, Value::float8);
				Ok(())
			}
			ColumnData::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = Int::zero();
					let mut has_value = false;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum = Int(sum.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						self.sums.insert(group.clone(), Value::Int(sum));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnData::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = Uint::zero();
					let mut has_value = false;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum = Uint(sum.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						self.sums.insert(group.clone(), Value::Uint(sum));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = Decimal::zero();
					let mut has_value = false;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum = Decimal(sum.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						self.sums.insert(group.clone(), Value::Decimal(sum));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
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
		let ty = self.input_type.take().unwrap_or(Type::Int8);
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnData::with_capacity(ty, self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data))
	}

	fn return_type(&self, input_type: &Type) -> Type {
		input_type.clone()
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}
}
