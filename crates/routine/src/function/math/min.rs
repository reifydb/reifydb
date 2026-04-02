// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::{
	Column,
	columns::Columns,
	data::ColumnData,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_type::{
	fragment::Fragment,
	value::{
		Value,
		decimal::Decimal,
		int::Int,
		r#type::{Type, input_types::InputTypes},
		uint::Uint,
	},
};

use crate::function::{Accumulator, Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Min {
	info: FunctionInfo,
}

impl Default for Min {
	fn default() -> Self {
		Self::new()
	}
}

impl Min {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::min"),
		}
	}
}

impl Function for Min {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar, FunctionCapability::Aggregate]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		for (i, col) in args.iter().enumerate() {
			if !col.get_type().is_number() {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: i,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: col.get_type(),
				});
			}
		}

		let row_count = args.row_count();
		let input_type = args[0].get_type();
		let mut data = ColumnData::with_capacity(input_type, row_count);

		for i in 0..row_count {
			let mut row_min: Option<Value> = None;
			for col in args.iter() {
				if col.data().is_defined(i) {
					let val = col.data().get_value(i);
					row_min = Some(match row_min {
						Some(current) if val < current => val,
						Some(current) => current,
						None => val,
					});
				}
			}
			data.push_value(row_min.unwrap_or(Value::none()));
		}

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), data)]))
	}

	fn accumulator(&self, _ctx: &FunctionContext) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(MinAccumulator::new()))
	}
}

struct MinAccumulator {
	pub mins: IndexMap<GroupKey, Value>,
	input_type: Option<Type>,
}

impl MinAccumulator {
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
			if let Some(v) = min {
				$self.mins.insert(group.clone(), $ctor(v));
			} else {
				$self.mins.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

impl Accumulator for MinAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), FunctionError> {
		let column = &args[0];
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
						if column.data().is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							min = Some(match min {
								Some(current) => f32::min(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = min {
						self.mins.insert(group.clone(), Value::float4(v));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnData::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut min: Option<f64> = None;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							min = Some(match min {
								Some(current) => f64::min(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = min {
						self.mins.insert(group.clone(), Value::float8(v));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
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
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							min = Some(match min {
								Some(current) if *val < current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = min {
						self.mins.insert(group.clone(), Value::Int(v));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
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
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							min = Some(match min {
								Some(current) if *val < current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = min {
						self.mins.insert(group.clone(), Value::Uint(v));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
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
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							min = Some(match min {
								Some(current) if *val < current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = min {
						self.mins.insert(group.clone(), Value::Decimal(v));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: Fragment::internal("math::min"),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnData), FunctionError> {
		let ty = self.input_type.take().unwrap_or(Type::Float8);
		let mut keys = Vec::with_capacity(self.mins.len());
		let mut data = ColumnData::with_capacity(ty, self.mins.len());

		for (key, min) in mem::take(&mut self.mins) {
			keys.push(key);
			data.push_value(min);
		}

		Ok((keys, data))
	}
}
