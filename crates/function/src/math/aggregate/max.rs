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

use crate::{
	AggregateFunction, AggregateFunctionContext,
	error::{AggregateFunctionError, AggregateFunctionResult},
};

pub struct Max {
	pub maxs: IndexMap<Vec<Value>, Value>,
	input_type: Option<Type>,
}

impl Max {
	pub fn new() -> Self {
		Self {
			maxs: IndexMap::new(),
			input_type: None,
		}
	}
}

macro_rules! max_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $ctor:expr) => {
		for (group, indices) in $groups.iter() {
			let mut max = None;
			for &i in indices {
				if $column.data().is_defined(i) {
					if let Some(&val) = $container.get(i) {
						max = Some(match max {
							Some(current) if val > current => val,
							Some(current) => current,
							None => val,
						});
					}
				}
			}
			match max {
				Some(v) => {
					$self.maxs.insert(group.clone(), $ctor(v));
				}
				None => {
					$self.maxs.entry(group.clone()).or_insert(Value::none());
				}
			}
		}
	};
}

impl AggregateFunction for Max {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;
		let (data, _bitvec) = column.data().unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnData::Int1(container) => {
				max_arm!(self, column, groups, container, Value::Int1);
				Ok(())
			}
			ColumnData::Int2(container) => {
				max_arm!(self, column, groups, container, Value::Int2);
				Ok(())
			}
			ColumnData::Int4(container) => {
				max_arm!(self, column, groups, container, Value::Int4);
				Ok(())
			}
			ColumnData::Int8(container) => {
				max_arm!(self, column, groups, container, Value::Int8);
				Ok(())
			}
			ColumnData::Int16(container) => {
				max_arm!(self, column, groups, container, Value::Int16);
				Ok(())
			}
			ColumnData::Uint1(container) => {
				max_arm!(self, column, groups, container, Value::Uint1);
				Ok(())
			}
			ColumnData::Uint2(container) => {
				max_arm!(self, column, groups, container, Value::Uint2);
				Ok(())
			}
			ColumnData::Uint4(container) => {
				max_arm!(self, column, groups, container, Value::Uint4);
				Ok(())
			}
			ColumnData::Uint8(container) => {
				max_arm!(self, column, groups, container, Value::Uint8);
				Ok(())
			}
			ColumnData::Uint16(container) => {
				max_arm!(self, column, groups, container, Value::Uint16);
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let mut max: Option<f32> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(&val) = container.get(i) {
								max = Some(match max {
									Some(current) => f32::max(current, val),
									None => val,
								});
							}
						}
					}
					match max {
						Some(v) => {
							self.maxs.insert(group.clone(), Value::float4(v));
						}
						None => {
							self.maxs.entry(group.clone()).or_insert(Value::none());
						}
					}
				}
				Ok(())
			}
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut max: Option<f64> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(&val) = container.get(i) {
								max = Some(match max {
									Some(current) => f64::max(current, val),
									None => val,
								});
							}
						}
					}
					match max {
						Some(v) => {
							self.maxs.insert(group.clone(), Value::float8(v));
						}
						None => {
							self.maxs.entry(group.clone()).or_insert(Value::none());
						}
					}
				}
				Ok(())
			}
			ColumnData::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Int> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								max = Some(match max {
									Some(current) if *val > current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match max {
						Some(v) => {
							self.maxs.insert(group.clone(), Value::Int(v));
						}
						None => {
							self.maxs.entry(group.clone()).or_insert(Value::none());
						}
					}
				}
				Ok(())
			}
			ColumnData::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Uint> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								max = Some(match max {
									Some(current) if *val > current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match max {
						Some(v) => {
							self.maxs.insert(group.clone(), Value::Uint(v));
						}
						None => {
							self.maxs.entry(group.clone()).or_insert(Value::none());
						}
					}
				}
				Ok(())
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Decimal> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								max = Some(match max {
									Some(current) if *val > current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match max {
						Some(v) => {
							self.maxs.insert(group.clone(), Value::Decimal(v));
						}
						None => {
							self.maxs.entry(group.clone()).or_insert(Value::none());
						}
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
		let ty = self.input_type.take().unwrap_or(Type::Float8);
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnData::with_capacity(ty, self.maxs.len());

		for (key, max) in mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(max);
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
