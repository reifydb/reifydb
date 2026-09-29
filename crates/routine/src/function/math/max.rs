// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	metrics::heap::HeapSize,
	value::column::{
		builder::ColumnBuilder,
		nulls::split_nulls,
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
		column_view::{ColumnView, ViewData},
		container::{decimal_array::decimal_at, wide_int_array::wides},
		decimal::Decimal,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::function::support::coerce::bare_type;

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let columns = args.iter().map(ColumnView::try_from).collect::<Result<Vec<_>, _>>()?;
		for (i, col) in columns.iter().enumerate() {
			if !col.get_type().is_number() {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: i,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: col.get_type(),
				});
			}
		}

		let row_count = ctx.row_count;
		let input_type = columns[0].get_type();
		let mut data = ColumnBuilder::with_capacity(input_type, row_count);

		for i in 0..row_count {
			let mut row_max: Option<Value> = None;
			for col in columns.iter() {
				if col.is_defined(i) {
					let val = col.get_value(i);
					row_max = Some(match row_max {
						Some(current) if val > current => val,
						Some(current) => current,
						None => val,
					});
				}
			}
			data.push_value(row_max.unwrap_or(Value::none()));
		}

		Ok(data.finish(ctx.fragment.text()))
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

	fn update(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError> {
		let column = ColumnView::try_from(&args[0])?;
		let (bare, _) = split_nulls(args[0].clone())?;
		let data = ColumnView::try_from(&bare)?;

		if self.input_type.is_none() {
			self.input_type = Some(bare_type(&data));
		}

		match &data.data {
			ViewData::Int1(container) => {
				max_arm!(self, column, groups, container.values(), Int1);
				Ok(())
			}
			ViewData::Int2(container) => {
				max_arm!(self, column, groups, container.values(), Int2);
				Ok(())
			}
			ViewData::Int4(container) => {
				max_arm!(self, column, groups, container.values(), Int4);
				Ok(())
			}
			ViewData::Int8(container) => {
				max_arm!(self, column, groups, container.values(), Int8);
				Ok(())
			}
			ViewData::Int16(container) => {
				let values = wides::<i128>(container);
				max_arm!(self, column, groups, values, Int16);
				Ok(())
			}
			ViewData::Uint1(container) => {
				max_arm!(self, column, groups, container.values(), Uint1);
				Ok(())
			}
			ViewData::Uint2(container) => {
				max_arm!(self, column, groups, container.values(), Uint2);
				Ok(())
			}
			ViewData::Uint4(container) => {
				max_arm!(self, column, groups, container.values(), Uint4);
				Ok(())
			}
			ViewData::Uint8(container) => {
				max_arm!(self, column, groups, container.values(), Uint8);
				Ok(())
			}
			ViewData::Uint16(container) => {
				let values = wides::<u128>(container);
				max_arm!(self, column, groups, values, Uint16);
				Ok(())
			}
			ViewData::Float4(container) => {
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
			ViewData::Float8(container) => {
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
			ViewData::Decimal(container) => {
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
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: self.function.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: data.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, (FieldRef, ArrayRef)), RoutineError> {
		let ty = self.input_type.take().unwrap_or(ValueType::Float8);
		let mut keys = Vec::with_capacity(self.maxs.len());
		let mut data = ColumnBuilder::with_capacity(ty, self.maxs.len());

		for (key, max) in mem::take(&mut self.maxs) {
			keys.push(key);
			data.push_value(max);
		}

		Ok((keys, data.finish(self.kind_name())))
	}
}
