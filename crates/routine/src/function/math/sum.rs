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

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Int8)
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		// SCALAR: Horizontal Sum (summing columns in each row)
		if args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		let row_count = args.row_count();
		let mut results = Vec::with_capacity(row_count);

		for i in 0..row_count {
			// Basic implementation: just use first arg for now or add them if possible
			// In a full implementation we would use a unified adder
			let val1 = args[0].get_value(i);
			results.push(Box::new(val1));
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::any(results))]))
	}
}

impl Function for Sum {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn accumulator(&self, _ctx: &mut FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(SumAccumulator::new()))
	}
}

struct SumAccumulator {
	pub sums: IndexMap<Vec<Value>, Value>,
	input_type: Option<Type>,
}

impl SumAccumulator {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
			input_type: None,
		}
	}
}

macro_rules! sum_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident) => {
		for (group, indices) in $groups.iter() {
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
				let merged = match $self.sums.swap_remove(group) {
					Some(Value::$variant(prev)) => prev + delta,
					_ => delta,
				};
				$self.sums.insert(group.clone(), Value::$variant(merged));
			} else {
				$self.sums.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

macro_rules! sum_arm_float {
	($self:expr, $column:expr, $groups:expr, $container:expr, $t:ty, $variant:ident, $ctor:expr) => {
		for (group, indices) in $groups.iter() {
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
				let merged = match $self.sums.swap_remove(group) {
					Some(Value::$variant(prev)) => prev.value() + delta,
					_ => delta,
				};
				$self.sums.insert(group.clone(), $ctor(merged));
			} else {
				$self.sums.entry(group.clone()).or_insert(Value::none());
			}
		}
	};
}

impl Accumulator for SumAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _bitvec) = column.unwrap_option();

		if self.input_type.is_none() {
			self.input_type = Some(data.get_type());
		}

		match data {
			ColumnBuffer::Int1(container) => {
				sum_arm!(self, column, groups, container, i8, Int1);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				sum_arm!(self, column, groups, container, i16, Int2);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				sum_arm!(self, column, groups, container, i32, Int4);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				sum_arm!(self, column, groups, container, i64, Int8);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				sum_arm!(self, column, groups, container, i128, Int16);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				sum_arm!(self, column, groups, container, u8, Uint1);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				sum_arm!(self, column, groups, container, u16, Uint2);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				sum_arm!(self, column, groups, container, u32, Uint4);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				sum_arm!(self, column, groups, container, u64, Uint8);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				sum_arm!(self, column, groups, container, u128, Uint16);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				sum_arm_float!(self, column, groups, container, f32, Float4, Value::float4);
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				sum_arm_float!(self, column, groups, container, f64, Float8, Value::float8);
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut delta = Int::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = Int(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.swap_remove(group) {
							Some(Value::Int(prev)) => Int(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group.clone(), Value::Int(merged));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut delta = Uint::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = Uint(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.swap_remove(group) {
							Some(Value::Uint(prev)) => Uint(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group.clone(), Value::Uint(merged));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut has_value = false;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = Decimal(delta.0 + &val.0);
							has_value = true;
						}
					}
					if has_value {
						let merged = match self.sums.swap_remove(group) {
							Some(Value::Decimal(prev)) => Decimal(prev.0 + &delta.0),
							_ => delta,
						};
						self.sums.insert(group.clone(), Value::Decimal(merged));
					} else {
						self.sums.entry(group.clone()).or_insert(Value::none());
					}
				}
				Ok(())
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: Fragment::internal("math::sum"),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError> {
		let ty = self.input_type.take().unwrap_or(Type::Int8);
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnBuffer::with_capacity(ty, self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			keys.push(key);
			data.push_value(sum);
		}

		Ok((keys, data))
	}
}
