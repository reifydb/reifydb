// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
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

use crate::routine::{Accumulator, FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Max {
	info: RoutineInfo,
}

impl Default for Max {
	fn default() -> Self {
		Self::new()
	}
}

impl Max {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::max"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Max {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		for (i, col) in args.iter().enumerate() {
			if !col.get_type().is_number() {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: i,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: col.get_type(),
				});
			}
		}

		let row_count = args.row_count();
		let input_type = args[0].get_type();
		let mut data = ColumnBuffer::with_capacity(input_type, row_count);

		for i in 0..row_count {
			let mut row_max: Option<Value> = None;
			for col in args.iter() {
				if col.data().is_defined(i) {
					let val = col.data().get_value(i);
					row_max = Some(match row_max {
						Some(current) if val > current => val,
						Some(current) => current,
						None => val,
					});
				}
			}
			data.push_value(row_max.unwrap_or(Value::none()));
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), data)]))
	}

	fn accumulator(&self, _ctx: &mut FunctionContext<'a>) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(MaxAccumulator::new()))
	}
}

struct MaxAccumulator {
	pub maxs: IndexMap<GroupKey, Value>,
	input_type: Option<Type>,
}

impl MaxAccumulator {
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
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						max = Some(match max {
							Some(current) if val > current => val,
							Some(current) => current,
							None => val,
						});
					}
				}
			}
			if let Some(v) = max {
				$self.maxs.insert(group.clone(), $ctor(v));
			} else {
				$self.maxs.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

impl Accumulator for MaxAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _bitvec) = column.unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnBuffer::Int1(container) => {
				max_arm!(self, column, groups, container, Value::Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				max_arm!(self, column, groups, container, Value::Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				max_arm!(self, column, groups, container, Value::Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				max_arm!(self, column, groups, container, Value::Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				max_arm!(self, column, groups, container, Value::Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				max_arm!(self, column, groups, container, Value::Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				max_arm!(self, column, groups, container, Value::Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				max_arm!(self, column, groups, container, Value::Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				max_arm!(self, column, groups, container, Value::Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				max_arm!(self, column, groups, container, Value::Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				for (group, indices) in groups.iter() {
					let mut max: Option<f32> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							max = Some(match max {
								Some(current) => f32::max(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = max {
						self.maxs.insert(group.clone(), Value::float4(v));
					} else {
						self.maxs.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut max: Option<f64> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							max = Some(match max {
								Some(current) => f64::max(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = max {
						self.maxs.insert(group.clone(), Value::float8(v));
					} else {
						self.maxs.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Int> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							max = Some(match max {
								Some(current) if *val > current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = max {
						self.maxs.insert(group.clone(), Value::Int(v));
					} else {
						self.maxs.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Uint> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							max = Some(match max {
								Some(current) if *val > current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = max {
						self.maxs.insert(group.clone(), Value::Uint(v));
					} else {
						self.maxs.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut max: Option<Decimal> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							max = Some(match max {
								Some(current) if *val > current => val.clone(),
								Some(current) => current,
								None => val.clone(),
							});
						}
					}
					if let Some(v) = max {
						self.maxs.insert(group.clone(), Value::Decimal(v));
					} else {
						self.maxs.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: Fragment::internal("math::max"),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError> {
		let ty = self.input_type.take().unwrap_or(Type::Float8);
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnBuffer::with_capacity(ty, self.maxs.len());

		for (key, max) in mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(max);
		}

		Ok((keys, data))
	}
}
