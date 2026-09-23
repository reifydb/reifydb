// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	metrics::heap::HeapSize,
	value::column::{
		ColumnWithName,
		buffer::ColumnBuffer,
		builder::ColumnBuilder,
		columns::Columns,
		view::group_by::{GroupId, GroupRows, GroupSlots},
	},
};
use reifydb_routine_abi::{
	Accumulator, Arity, Function, FunctionKind, LiteralArgument, Routine, RoutineInfo, context::FunctionContext,
	error::RoutineError,
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		container::decimal_array::{decimal_at, int_at, u128s, uint_at},
		decimal::Decimal,
		int::Int,
		uint::Uint,
		value_type::{ValueType, input_types::InputTypes},
	},
};

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

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().cloned().unwrap_or(ValueType::Float8)
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
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
		let mut data = ColumnBuilder::with_capacity(input_type, row_count);

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

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data.finish())]))
	}
}

impl Function for Max {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn arity(&self) -> Arity {
		Arity::AtLeast(1)
	}

	fn accumulator(
		&self,
		ctx: &mut FunctionContext<'_>,
		_literals: &[LiteralArgument],
	) -> Result<Option<Box<dyn Accumulator>>, RoutineError> {
		Ok(Some(Box::new(MaxAccumulator::new(ctx.fragment.clone()))))
	}
}

struct MaxAccumulator {
	function: Fragment,
	pub maxs: GroupSlots<Value>,
	input_type: Option<ValueType>,
}

impl MaxAccumulator {
	pub fn new(function: Fragment) -> Self {
		Self {
			function,
			maxs: GroupSlots::new(),
			input_type: None,
		}
	}
}

macro_rules! max_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $variant:ident) => {
		for &(group, ref indices) in $groups.iter() {
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
				let merged = match $self.maxs.remove(group) {
					Some(Value::$variant(prev)) if prev > v => prev,
					_ => v,
				};
				$self.maxs.insert(group, Value::$variant(merged));
			} else {
				$self.maxs.or_insert(group, Value::none());
			}
		}
	};
}

impl Accumulator for MaxAccumulator {
	fn heap_size(&self) -> usize {
		self.maxs.heap_size()
	}

	fn update(&mut self, args: &Columns, groups: &GroupRows) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _) = column.clone().split_nulls();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match &data {
			ColumnBuffer::Int1(container) => {
				max_arm!(self, column, groups, container.values(), Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				max_arm!(self, column, groups, container.values(), Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				max_arm!(self, column, groups, container.values(), Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				max_arm!(self, column, groups, container.values(), Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				max_arm!(self, column, groups, container.values(), Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				max_arm!(self, column, groups, container.values(), Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				max_arm!(self, column, groups, container.values(), Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				max_arm!(self, column, groups, container.values(), Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				max_arm!(self, column, groups, container.values(), Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				let values = u128s(container);
				max_arm!(self, column, groups, values, Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				for &(group, ref indices) in groups.iter() {
					let mut max: Option<f32> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.values().get(i)
						{
							max = Some(match max {
								Some(current) => f32::max(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = max {
						let merged = match self.maxs.remove(group) {
							Some(Value::Float4(prev)) => f32::max(prev.value(), v),
							_ => v,
						};
						self.maxs.insert(group, Value::float4(merged));
					} else {
						self.maxs.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				for &(group, ref indices) in groups.iter() {
					let mut max: Option<f64> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.values().get(i)
						{
							max = Some(match max {
								Some(current) => f64::max(current, val),
								None => val,
							});
						}
					}
					if let Some(v) = max {
						let merged = match self.maxs.remove(group) {
							Some(Value::Float8(prev)) => f64::max(prev.value(), v),
							_ => v,
						};
						self.maxs.insert(group, Value::float8(merged));
					} else {
						self.maxs.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Int(container) => {
				for &(group, ref indices) in groups.iter() {
					let mut max: Option<Int> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = int_at(container, i)
						{
							max = Some(match max {
								Some(current) if val > current => val,
								Some(current) => current,
								None => val,
							});
						}
					}
					if let Some(v) = max {
						let merged = match self.maxs.remove(group) {
							Some(Value::Int(prev)) if prev > v => prev,
							_ => v,
						};
						self.maxs.insert(group, Value::Int(merged));
					} else {
						self.maxs.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint(container) => {
				for &(group, ref indices) in groups.iter() {
					let mut max: Option<Uint> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = uint_at(container, i)
						{
							max = Some(match max {
								Some(current) if val > current => val,
								Some(current) => current,
								None => val,
							});
						}
					}
					if let Some(v) = max {
						let merged = match self.maxs.remove(group) {
							Some(Value::Uint(prev)) if prev > v => prev,
							_ => v,
						};
						self.maxs.insert(group, Value::Uint(merged));
					} else {
						self.maxs.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal(container) => {
				for &(group, ref indices) in groups.iter() {
					let mut max: Option<Decimal> = None;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = decimal_at(container, i)
						{
							max = Some(match max {
								Some(current) if val > current => val,
								Some(current) => current,
								None => val,
							});
						}
					}
					if let Some(v) = max {
						let merged = match self.maxs.remove(group) {
							Some(Value::Decimal(prev)) if prev > v => prev,
							_ => v,
						};
						self.maxs.insert(group, Value::Decimal(merged));
					} else {
						self.maxs.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: self.function.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, ColumnBuffer), RoutineError> {
		let ty = self.input_type.take().unwrap_or(ValueType::Float8);
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnBuilder::with_capacity(ty, self.maxs.len());

		for (key, max) in mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(max);
		}

		Ok((keys, data.finish()))
	}
}
