// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter, mem};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	metrics::heap::HeapSize,
	value::column::{
		builder::ColumnBuilder,
		factory::any_optional,
		nulls::split_nulls,
		view::group_by::{GroupId, GroupRows, GroupSlots},
	},
};
use reifydb_routine_abi::{
	Accumulator, AggregateFunctionCapability, Arity, Function, FunctionKind, LiteralArgument, Routine, RoutineInfo,
	context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	error::TypeError,
	fragment::Fragment,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{decimal_array::decimals, wide_int_array::wides},
		decimal::Decimal,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::function::support::coerce::bare_type;

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
		sum_type(input_types.first().cloned().unwrap_or(ValueType::Int8), iter::empty())
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = ctx.row_count;
		let mut results = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let val1 = data.get_value(i);
			results.push(match val1 {
				Value::None {
					..
				} => None,
				value => Some(value),
			});
		}

		Ok(any_optional(ctx.fragment.text(), results))
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

fn sum_type<'a>(input: ValueType, sums: impl Iterator<Item = &'a Value>) -> ValueType {
	match input {
		ValueType::Decimal {
			scale,
			..
		} => {
			let widest = sums
				.filter_map(|sum| match sum {
					Value::Decimal(value) => Some(value.scale()),
					_ => None,
				})
				.fold(scale.value(), u8::max);
			ValueType::decimal(Precision::MAX, Scale::new(widest))
		}
		other => other,
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
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident) => {{
		let function = $self.function.clone();
		let overflow = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: ValueType::$variant,
				fragment: function.clone(),
				descriptor: None,
			}
			.into()
		};
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta = delta.checked_add(val).ok_or_else(overflow)?;
						has_value = true;
					}
				}
			}
			if has_value {
				let merged = match $self.sums.remove(group) {
					Some(Value::$variant(prev)) => prev.checked_add(delta).ok_or_else(overflow)?,
					_ => delta,
				};
				$self.sums.insert(group, Value::$variant(merged));
			} else {
				$self.sums.or_insert(group, Value::none());
			}
		}
	}};
}

macro_rules! sub_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident) => {{
		let function = $self.function.clone();
		let overflow = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: ValueType::$variant,
				fragment: function.clone(),
				descriptor: None,
			}
			.into()
		};
		for &(group, ref indices) in $groups.iter() {
			let mut delta: $t = Default::default();
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i) {
					if let Some(&val) = $container.get(i) {
						delta = delta.checked_add(val).ok_or_else(overflow)?;
						has_value = true;
					}
				}
			}
			if has_value && let Some(Value::$variant(prev)) = $self.sums.remove(group) {
				let remaining = prev.checked_sub(delta).ok_or_else(overflow)?;
				$self.sums.insert(group, Value::$variant(remaining));
			}
		}
	}};
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

macro_rules! sum_family_arm {
	($self:expr, $column:expr, $groups:expr, $values:expr, $zero:expr, $variant:ident, $target:expr) => {{
		let function = $self.function.clone();
		let target = $target;
		let overflow = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: target.clone(),
				fragment: function.clone(),
				descriptor: None,
			}
			.into()
		};
		for &(group, ref indices) in $groups.iter() {
			let mut delta = $zero;
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i)
					&& let Some(val) = $values.get(i)
				{
					delta = delta.checked_add(val).ok_or_else(overflow)?;
					has_value = true;
				}
			}
			if has_value {
				let merged = match $self.sums.remove(group) {
					Some(Value::$variant(prev)) => prev.checked_add(&delta).ok_or_else(overflow)?,
					_ => delta,
				};
				$self.sums.insert(group, Value::$variant(merged));
			} else {
				$self.sums.or_insert(group, Value::none());
			}
		}
	}};
}

macro_rules! sub_family_arm {
	($self:expr, $column:expr, $groups:expr, $values:expr, $zero:expr, $variant:ident, $target:expr) => {{
		let function = $self.function.clone();
		let target = $target;
		let overflow = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: target.clone(),
				fragment: function.clone(),
				descriptor: None,
			}
			.into()
		};
		for &(group, ref indices) in $groups.iter() {
			let mut delta = $zero;
			let mut has_value = false;
			for &i in indices {
				if $column.is_defined(i)
					&& let Some(val) = $values.get(i)
				{
					delta = delta.checked_add(val).ok_or_else(overflow)?;
					has_value = true;
				}
			}
			if has_value && let Some(Value::$variant(prev)) = $self.sums.remove(group) {
				let remaining = prev.checked_sub(&delta).ok_or_else(overflow)?;
				$self.sums.insert(group, Value::$variant(remaining));
			}
		}
	}};
}

impl Accumulator for SumAccumulator {
	fn heap_size(&self) -> usize {
		self.sums.heap_size()
	}

	fn update(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError> {
		let column = ColumnView::try_from(&args[0])?;
		let (bare, _) = split_nulls(args[0].clone())?;
		let data = ColumnView::try_from(&bare)?;

		if self.input_type.is_none() {
			self.input_type = Some(bare_type(&data));
		}

		match &data.data {
			ViewData::Int1(container) => {
				sum_arm!(self, column, groups, container.values(), i8, Int1);
				Ok(())
			}
			ViewData::Int2(container) => {
				sum_arm!(self, column, groups, container.values(), i16, Int2);
				Ok(())
			}
			ViewData::Int4(container) => {
				sum_arm!(self, column, groups, container.values(), i32, Int4);
				Ok(())
			}
			ViewData::Int8(container) => {
				sum_arm!(self, column, groups, container.values(), i64, Int8);
				Ok(())
			}
			ViewData::Int16(container) => {
				let values = wides::<i128>(container);
				sum_arm!(self, column, groups, values, i128, Int16);
				Ok(())
			}
			ViewData::Uint1(container) => {
				sum_arm!(self, column, groups, container.values(), u8, Uint1);
				Ok(())
			}
			ViewData::Uint2(container) => {
				sum_arm!(self, column, groups, container.values(), u16, Uint2);
				Ok(())
			}
			ViewData::Uint4(container) => {
				sum_arm!(self, column, groups, container.values(), u32, Uint4);
				Ok(())
			}
			ViewData::Uint8(container) => {
				sum_arm!(self, column, groups, container.values(), u64, Uint8);
				Ok(())
			}
			ViewData::Uint16(container) => {
				let values = wides::<u128>(container);
				sum_arm!(self, column, groups, values, u128, Uint16);
				Ok(())
			}
			ViewData::Float4(container) => {
				sum_arm_float!(self, column, groups, container.values(), f32, Float4, Value::float4);
				Ok(())
			}
			ViewData::Float8(container) => {
				sum_arm_float!(self, column, groups, container.values(), f64, Float8, Value::float8);
				Ok(())
			}
			ViewData::Decimal(container) => {
				let values = decimals(container);
				let target = ValueType::decimal(Precision::MAX, container.scale());
				sum_family_arm!(self, column, groups, values, Decimal::zero(), Decimal, target);
				Ok(())
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: self.function.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: data.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, (FieldRef, ArrayRef)), RoutineError> {
		let sums: Vec<(GroupId, Value)> = mem::take(&mut self.sums).into_iter().collect();
		let ty = sum_type(self.input_type.take().unwrap_or(ValueType::Int8), sums.iter().map(|(_, sum)| sum));
		let mut keys = Vec::with_capacity(sums.len());
		let mut data = ColumnBuilder::with_capacity(ty, sums.len());

		for (key, sum) in sums {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data.finish(self.kind_name())))
	}

	fn kind_name(&self) -> &'static str {
		"math::sum"
	}

	fn retract(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError> {
		let column = ColumnView::try_from(&args[0])?;
		let (bare, _) = split_nulls(args[0].clone())?;
		let data = ColumnView::try_from(&bare)?;

		if self.input_type.is_none() {
			self.input_type = Some(bare_type(&data));
		}

		match &data.data {
			ViewData::Int1(container) => {
				sub_arm!(self, column, groups, container.values(), i8, Int1);
				Ok(())
			}
			ViewData::Int2(container) => {
				sub_arm!(self, column, groups, container.values(), i16, Int2);
				Ok(())
			}
			ViewData::Int4(container) => {
				sub_arm!(self, column, groups, container.values(), i32, Int4);
				Ok(())
			}
			ViewData::Int8(container) => {
				sub_arm!(self, column, groups, container.values(), i64, Int8);
				Ok(())
			}
			ViewData::Int16(container) => {
				let values = wides::<i128>(container);
				sub_arm!(self, column, groups, values, i128, Int16);
				Ok(())
			}
			ViewData::Uint1(container) => {
				sub_arm!(self, column, groups, container.values(), u8, Uint1);
				Ok(())
			}
			ViewData::Uint2(container) => {
				sub_arm!(self, column, groups, container.values(), u16, Uint2);
				Ok(())
			}
			ViewData::Uint4(container) => {
				sub_arm!(self, column, groups, container.values(), u32, Uint4);
				Ok(())
			}
			ViewData::Uint8(container) => {
				sub_arm!(self, column, groups, container.values(), u64, Uint8);
				Ok(())
			}
			ViewData::Uint16(container) => {
				let values = wides::<u128>(container);
				sub_arm!(self, column, groups, values, u128, Uint16);
				Ok(())
			}
			ViewData::Float4(container) => {
				sub_arm_float!(self, column, groups, container.values(), f32, Float4, Value::float4);
				Ok(())
			}
			ViewData::Float8(container) => {
				sub_arm_float!(self, column, groups, container.values(), f64, Float8, Value::float8);
				Ok(())
			}
			ViewData::Decimal(container) => {
				let values = decimals(container);
				let target = ValueType::decimal(Precision::MAX, container.scale());
				sub_family_arm!(self, column, groups, values, Decimal::zero(), Decimal, target);
				Ok(())
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: self.function.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: data.get_type(),
			}),
		}
	}
}
