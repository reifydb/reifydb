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

use crate::routine::{
	Accumulator, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};

pub struct Min {
	info: RoutineInfo,
}

impl Default for Min {
	fn default() -> Self {
		Self::new()
	}
}

impl Min {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::min"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Min {
	fn info(&self) -> &RoutineInfo {
		&self.info
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
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		for (i, col) in args.iter().enumerate() {
			if !col.get_type().is_number() {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
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

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
	}
}

impl Function for Min {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn accumulator(&self, _ctx: &mut FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
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
	($self:expr, $column:expr, $groups:expr, $container:expr, $variant:ident) => {
		for (group, indices) in $groups.iter() {
			let mut min = None;
			for &i in indices {
				if $column.is_defined(i) {
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
				let merged = match $self.mins.swap_remove(group) {
					Some(Value::$variant(prev)) if prev < v => prev,
					_ => v,
				};
				$self.mins.insert(group.clone(), Value::$variant(merged));
			} else {
				$self.mins.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

impl Accumulator for MinAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _bitvec) = column.unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnBuffer::Int1(container) => {
				min_arm!(self, column, groups, container, Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				min_arm!(self, column, groups, container, Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				min_arm!(self, column, groups, container, Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				min_arm!(self, column, groups, container, Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				min_arm!(self, column, groups, container, Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				min_arm!(self, column, groups, container, Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				min_arm!(self, column, groups, container, Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				min_arm!(self, column, groups, container, Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				min_arm!(self, column, groups, container, Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				min_arm!(self, column, groups, container, Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				for (group, indices) in groups.iter() {
					let mut min: Option<f32> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							min = Some(match min {
								Some(current) => f32::min(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = min {
						let merged = match self.mins.swap_remove(group) {
							Some(Value::Float4(prev)) => f32::min(prev.value(), v),
							_ => v,
						};
						self.mins.insert(group.clone(), Value::float4(merged));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				for (group, indices) in groups.iter() {
					let mut min: Option<f64> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							min = Some(match min {
								Some(current) => f64::min(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = min {
						let merged = match self.mins.swap_remove(group) {
							Some(Value::Float8(prev)) => f64::min(prev.value(), v),
							_ => v,
						};
						self.mins.insert(group.clone(), Value::float8(merged));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut min: Option<Int> = None;
					for &i in indices {
						if column.is_defined(i)
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
						let merged = match self.mins.swap_remove(group) {
							Some(Value::Int(prev)) if prev < v => prev,
							_ => v,
						};
						self.mins.insert(group.clone(), Value::Int(merged));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut min: Option<Uint> = None;
					for &i in indices {
						if column.is_defined(i)
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
						let merged = match self.mins.swap_remove(group) {
							Some(Value::Uint(prev)) if prev < v => prev,
							_ => v,
						};
						self.mins.insert(group.clone(), Value::Uint(merged));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut min: Option<Decimal> = None;
					for &i in indices {
						if column.is_defined(i)
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
						let merged = match self.mins.swap_remove(group) {
							Some(Value::Decimal(prev)) if prev < v => prev,
							_ => v,
						};
						self.mins.insert(group.clone(), Value::Decimal(merged));
					} else {
						self.mins.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: Fragment::internal("math::min"),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError> {
		let ty = self.input_type.take().unwrap_or(Type::Float8);
		let mut keys = Vec::with_capacity(self.mins.len());
		let mut data = ColumnBuffer::with_capacity(ty, self.mins.len());

		for (key, min) in mem::take(&mut self.mins) {
			keys.push(key);
			data.push_value(min);
		}

		Ok((keys, data))
	}
}
