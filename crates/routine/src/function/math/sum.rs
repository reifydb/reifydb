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
	Accumulator, AggregateFunctionCapability, Arity, Function, FunctionKind, LiteralArgument, Routine, RoutineInfo,
	context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		container::{
			bignum_array::{decimal_at, int_at, uint_at},
			decimal_array::u128s,
		},
		decimal::Decimal,
		int::Int,
		uint::Uint,
		value_type::{ValueType, input_types::InputTypes},
	},
};

pub struct Sum {
	info: RoutineInfo,
}

impl Default for Sum {
	fn default() -> Self {
		Self::new()
	}
}

impl Sum {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sum"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Sum {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().cloned().unwrap_or(ValueType::Int8)
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let row_count = args.row_count();
		let mut results = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let val1 = args[0].get_value(i);
			results.push(val1);
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::any(results))]))
	}
}

impl Function for Sum {
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
		Ok(Some(Box::new(SumAccumulator::new(ctx.fragment.clone()))))
	}

	fn aggregate_capabilities(&self) -> &[AggregateFunctionCapability] {
		&[AggregateFunctionCapability::Retractable]
	}
}

struct SumAccumulator {
	function: Fragment,
	pub sums: GroupSlots<Value>,
	input_type: Option<ValueType>,
}

impl SumAccumulator {
	pub fn new(function: Fragment) -> Self {
		Self {
			function,
			sums: GroupSlots::new(),
			input_type: None,
		}
	}
}

macro_rules! sum_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident) => {
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta += val;
						has_value = true;
					}
				}
			}
			if has_value {
				let merged = match $self.sums.remove(group) {
					Some(Value::$variant(prev)) => prev + delta,
					_ => delta,
				};
				$self.sums.insert(group, Value::$variant(merged));
			} else {
				$self.sums.or_insert(group, Value::none());
			}
		}
	};
}

macro_rules! sub_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident) => {
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta += val;
						has_value = true;
					}
				}
			}
			if has_value && let Some(Value::$variant(prev)) = $self.sums.remove(group) {
				$self.sums.insert(group, Value::$variant(prev - delta));
			}
		}
	};
}

macro_rules! sum_arm_float {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident, $ctor:expr) => {
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta += val;
						has_value = true;
					}
				}
			}
			if has_value {
				let merged = match $self.sums.remove(group) {
					Some(Value::$variant(prev)) => prev.value() + delta,
					_ => delta,
				};
				$self.sums.insert(group, $ctor(merged));
			} else {
				$self.sums.or_insert(group, Value::none());
			}
		}
	};
}

macro_rules! sub_arm_float {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident, $ctor:expr) => {
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta += val;
						has_value = true;
					}
				}
			}
			if has_value && let Some(Value::$variant(prev)) = $self.sums.remove(group) {
				$self.sums.insert(group, $ctor(prev.value() - delta));
			}
		}
	};
}

impl Accumulator for SumAccumulator {
	fn heap_size(&self) -> usize {
		self.sums.heap_size()
	}

	fn update(&mut self, args: &Columns, groups: &GroupRows) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _) = column.clone().split_nulls();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match &data {
			ColumnBuffer::Int1(container) => {
				sum_arm!(self, column, groups, container.values(), i8, Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				sum_arm!(self, column, groups, container.values(), i16, Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				sum_arm!(self, column, groups, container.values(), i32, Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				sum_arm!(self, column, groups, container.values(), i64, Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				sum_arm!(self, column, groups, container.values(), i128, Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				sum_arm!(self, column, groups, container.values(), u8, Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				sum_arm!(self, column, groups, container.values(), u16, Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				sum_arm!(self, column, groups, container.values(), u32, Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				sum_arm!(self, column, groups, container.values(), u64, Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				let values = u128s(container);
				sum_arm!(self, column, groups, values, u128, Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				sum_arm_float!(self, column, groups, container.values(), f32, Float4, Value::float4);
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				sum_arm_float!(self, column, groups, container.values(), f64, Float8, Value::float8);
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Int::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = int_at(container, i)
						{
							delta = Int(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.remove(group) {
							Some(Value::Int(prev)) => Int(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group, Value::Int(merged));
					} else {
						self.sums.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Uint::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = uint_at(container, i)
						{
							delta = Uint(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.remove(group) {
							Some(Value::Uint(prev)) => Uint(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group, Value::Uint(merged));
					} else {
						self.sums.or_insert(group, Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = decimal_at(container, i)
						{
							delta = Decimal(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.remove(group) {
							Some(Value::Decimal(prev)) => Decimal(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group, Value::Decimal(merged));
					} else {
						self.sums.or_insert(group, Value::none());
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
		let ty = self.input_type.take().unwrap_or(ValueType::Int8);
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnBuilder::with_capacity(ty, self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data.finish()))
	}

	fn kind_name(&self) -> &'static str {
		"math::sum"
	}

	fn retract(&mut self, args: &Columns, groups: &GroupRows) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _) = column.clone().split_nulls();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match &data {
			ColumnBuffer::Int1(container) => {
				sub_arm!(self, column, groups, container.values(), i8, Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				sub_arm!(self, column, groups, container.values(), i16, Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				sub_arm!(self, column, groups, container.values(), i32, Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				sub_arm!(self, column, groups, container.values(), i64, Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				sub_arm!(self, column, groups, container.values(), i128, Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				sub_arm!(self, column, groups, container.values(), u8, Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				sub_arm!(self, column, groups, container.values(), u16, Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				sub_arm!(self, column, groups, container.values(), u32, Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				sub_arm!(self, column, groups, container.values(), u64, Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				let values = u128s(container);
				sub_arm!(self, column, groups, values, u128, Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				sub_arm_float!(self, column, groups, container.values(), f32, Float4, Value::float4);
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				sub_arm_float!(self, column, groups, container.values(), f64, Float8, Value::float8);
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Int::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = int_at(container, i)
						{
							delta = Int(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value && let Some(Value::Int(prev)) = self.sums.remove(group) {
						self.sums.insert(group, Value::Int(Int(prev.0 - &delta.0)));
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Uint::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = uint_at(container, i)
						{
							delta = Uint(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value && let Some(Value::Uint(prev)) = self.sums.remove(group) {
						self.sums.insert(group, Value::Uint(Uint(prev.0 - &delta.0)));
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = decimal_at(container, i)
						{
							delta = Decimal(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value && let Some(Value::Decimal(prev)) = self.sums.remove(group) {
						self.sums.insert(group, Value::Decimal(Decimal(prev.0 - &delta.0)));
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

	fn peek(&self, group: GroupId) -> Option<Value> {
		self.sums.get(group).cloned()
	}

	fn seed(&mut self, group: GroupId, value: Value) -> Result<(), RoutineError> {
		if matches!(value, Value::None { .. }) {
			return Ok(());
		}
		if self.input_type.is_none() {
			self.input_type = Some(value.get_type());
		}
		self.sums.insert(group, value);
		Ok(())
	}
}
