// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_catalog::function::{
	AggregateFunction, AggregateFunctionContext,
	error::{AggregateFunctionError, AggregateFunctionResult},
};
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{
	Value,
	decimal::Decimal,
	int::Int,
	r#type::{Type, input_types::InputTypes},
	uint::Uint,
};

pub struct Min {
	pub mins: IndexMap<Vec<Value>, Value>,
	input_type: Option<Type>,
}

impl Min {
	pub fn new() -> Self {
		Self {
			mins: IndexMap::new(),
			input_type: None,
		}
	}
}

macro_rules! min_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $ctor:expr) => {
		for (group, indices) in $groups.iter() {
			let mut min = None;
			for &i in indices {
				if $column.data().is_defined(i) {
					if let Some(&val) = $container.get(i) {
						min = Some(match min {
							Some(current) if val < current => val,
							Some(current) => current,
							None => val,
						});
					}
				}
			}
			match min {
				Some(v) => {
					$self.mins.insert(group.clone(), $ctor(v));
				}
				None => {
					$self.mins.entry(group.clone()).or_insert(Value::none());
				}
			}
		}
	};
}

impl AggregateFunction for Min {
	fn aggregate(&mut self, ctx: AggregateFunctionContext) -> AggregateFunctionResult<()> {
		let column = ctx.column;
		let groups = &ctx.groups;
		let (data, _bitvec) = column.data().unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnData::Int1(container) => {
				min_arm!(self, column, groups, container, Value::Int1);
				Ok(())
			}
			ColumnData::Int2(container) => {
				min_arm!(self, column, groups, container, Value::Int2);
				Ok(())
			}
			ColumnData::Int4(container) => {
				min_arm!(self, column, groups, container, Value::Int4);
				Ok(())
			}
			ColumnData::Int8(container) => {
				min_arm!(self, column, groups, container, Value::Int8);
				Ok(())
			}
			ColumnData::Int16(container) => {
				min_arm!(self, column, groups, container, Value::Int16);
				Ok(())
			}
			ColumnData::Uint1(container) => {
				min_arm!(self, column, groups, container, Value::Uint1);
				Ok(())
			}
			ColumnData::Uint2(container) => {
				min_arm!(self, column, groups, container, Value::Uint2);
				Ok(())
			}
			ColumnData::Uint4(container) => {
				min_arm!(self, column, groups, container, Value::Uint4);
				Ok(())
			}
			ColumnData::Uint8(container) => {
				min_arm!(self, column, groups, container, Value::Uint8);
				Ok(())
			}
			ColumnData::Uint16(container) => {
				min_arm!(self, column, groups, container, Value::Uint16);
				Ok(())
			}
			ColumnData::Float4(container) => {
				for (group, indices) in groups.iter() {
					let mut min: Option<f32> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(&val) = container.get(i) {
								min = Some(match min {
									Some(current) => f32::min(current, val),
									None => val,
								});
							}
						}
					}
					match min {
						Some(v) => {
							self.mins.insert(group.clone(), Value::float4(v));
						}
						None => {
							self.mins.entry(group.clone()).or_insert(Value::none());
						}
					}
				}
				Ok(())
			}
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut min: Option<f64> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(&val) = container.get(i) {
								min = Some(match min {
									Some(current) => f64::min(current, val),
									None => val,
								});
							}
						}
					}
					match min {
						Some(v) => {
							self.mins.insert(group.clone(), Value::float8(v));
						}
						None => {
							self.mins.entry(group.clone()).or_insert(Value::none());
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
					let mut min: Option<Int> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								min = Some(match min {
									Some(current) if *val < current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match min {
						Some(v) => {
							self.mins.insert(group.clone(), Value::Int(v));
						}
						None => {
							self.mins.entry(group.clone()).or_insert(Value::none());
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
					let mut min: Option<Uint> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								min = Some(match min {
									Some(current) if *val < current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match min {
						Some(v) => {
							self.mins.insert(group.clone(), Value::Uint(v));
						}
						None => {
							self.mins.entry(group.clone()).or_insert(Value::none());
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
					let mut min: Option<Decimal> = None;
					for &i in indices {
						if column.data().is_defined(i) {
							if let Some(val) = container.get(i) {
								min = Some(match min {
									Some(current) if *val < current => val.clone(),
									Some(current) => current,
									None => val.clone(),
								});
							}
						}
					}
					match min {
						Some(v) => {
							self.mins.insert(group.clone(), Value::Decimal(v));
						}
						None => {
							self.mins.entry(group.clone()).or_insert(Value::none());
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
		let mut keys = Vec::with_capacity(self.mins.len());
		let mut data = ColumnData::with_capacity(ty, self.mins.len());

		for (key, min) in mem::take(&mut self.mins) {
			keys.push(key);
			data.push_value(min);
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
