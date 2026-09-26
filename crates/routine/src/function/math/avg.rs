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
	error::TypeError,
	fragment::Fragment,
	value::{
		constraint::{precision::Precision, scale::Scale},
		container::decimal_array::{decimals, u128s},
		decimal::Decimal,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::function::support::numeric::MIN_DIVISION_SCALE;

pub struct Avg {
	info: RoutineInfo,
}

impl Default for Avg {
	fn default() -> Self {
		Self::new()
	}
}

impl Avg {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::avg"),
		}
	}
}

fn avg_decimal_type(input_scale: u8) -> ValueType {
	ValueType::decimal(Precision::MAX, Scale::new(input_scale.max(MIN_DIVISION_SCALE)))
}

fn avg_return_type(input_type: &ValueType) -> ValueType {
	match input_type {
		ValueType::Float4 => ValueType::Float4,
		ValueType::Float8 => ValueType::Float8,
		ValueType::Decimal {
			scale,
			..
		} => avg_decimal_type(scale.value()),
		_ => avg_decimal_type(0),
	}
}

fn avg_overflow(function: &Fragment, input_scale: u8) -> RoutineError {
	TypeError::NumberOutOfRange {
		target: avg_decimal_type(input_scale),
		fragment: function.clone(),
		descriptor: None,
	}
	.into()
}

fn average(function: &Fragment, sum: &Decimal, count: u64, input_scale: u8) -> Result<Decimal, RoutineError> {
	sum.checked_div(&Decimal::from(count)).ok_or_else(|| avg_overflow(function, input_scale))
}

fn average_column(input_scale: u8, values: Vec<Decimal>, valids: Vec<bool>) -> ColumnBuffer {
	let ValueType::Decimal {
		precision,
		scale,
	} = avg_decimal_type(values.iter().map(Decimal::scale).fold(input_scale, u8::max))
	else {
		unreachable!("an average of integers or decimals is a decimal")
	};
	ColumnBuffer::decimal_with_bitvec(precision, scale, values, valids)
}

macro_rules! exec_int_arm {
	($container:expr, $row_count:expr, $sums:expr, $counts:expr, $overflow:expr) => {
		for i in 0..$row_count {
			if let Some(value) = $container.get(i) {
				$sums[i] = $sums[i].checked_add(&Decimal::from(value.clone())).ok_or_else($overflow)?;
				$counts[i] += 1;
			}
		}
	};
}

macro_rules! acc_int_arm {
	($sums:expr, $counts:expr, $column:expr, $groups:expr, $container:expr, $overflow:expr) => {
		for &(group, ref indices) in $groups.iter() {
			let mut delta = Decimal::zero();
			let mut count = 0u64;
			for &i in indices {
				if $column.is_defined(i)
					&& let Some(val) = $container.get(i)
				{
					delta = delta.checked_add(&Decimal::from(val.clone())).ok_or_else($overflow)?;
					count += 1;
				}
			}
			if count > 0 {
				let merged = match $sums.remove(group) {
					Some(prev) => prev.checked_add(&delta).ok_or_else($overflow)?,
					None => delta,
				};
				$sums.insert(group, merged);
				*$counts.or_insert(group, 0) += count;
			} else {
				$sums.or_insert(group, Decimal::zero());
				$counts.or_insert(group, 0);
			}
		}
	};
}

impl<'a> Routine<FunctionContext<'a>> for Avg {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().map(avg_return_type).unwrap_or_else(|| avg_decimal_type(0))
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let row_count = args.row_count();
		let input_type = args[0].get_type();
		let result_type = avg_return_type(&input_type);

		match result_type {
			ValueType::Float4 => execute_float4(ctx, args, row_count),
			ValueType::Float8 => execute_float8(ctx, args, row_count),
			_ => execute_decimal(ctx, args, row_count),
		}
	}
}

fn execute_float4<'a>(
	ctx: &mut FunctionContext<'a>,
	args: &Columns,
	row_count: usize,
) -> Result<Columns, RoutineError> {
	let mut sums = vec![0.0f32; row_count];
	let mut counts = vec![0u32; row_count];

	for col in args.iter() {
		let data = col.data();
		if let ColumnBuffer::Float4(container) = data {
			for i in 0..row_count {
				if let Some(value) = container.values().get(i) {
					sums[i] += *value;
					counts[i] += 1;
				}
			}
		} else {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Float4],
				actual: data.get_type(),
			});
		}
	}

	let mut data = Vec::with_capacity(row_count);
	let mut valids = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if counts[i] > 0 {
			data.push(sums[i] / counts[i] as f32);
			valids.push(true);
		} else {
			data.push(0.0);
			valids.push(false);
		}
	}

	Ok(Columns::new(vec![ColumnWithName::new(
		ctx.fragment.clone(),
		ColumnBuffer::float4_with_bitvec(data, valids),
	)]))
}

fn execute_float8<'a>(
	ctx: &mut FunctionContext<'a>,
	args: &Columns,
	row_count: usize,
) -> Result<Columns, RoutineError> {
	let mut sums = vec![0.0f64; row_count];
	let mut counts = vec![0u32; row_count];

	for col in args.iter() {
		let data = col.data();
		if let ColumnBuffer::Float8(container) = data {
			for i in 0..row_count {
				if let Some(value) = container.values().get(i) {
					sums[i] += *value;
					counts[i] += 1;
				}
			}
		} else {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Float8],
				actual: data.get_type(),
			});
		}
	}

	let mut data = Vec::with_capacity(row_count);
	let mut valids = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if counts[i] > 0 {
			data.push(sums[i] / counts[i] as f64);
			valids.push(true);
		} else {
			data.push(0.0);
			valids.push(false);
		}
	}

	Ok(Columns::new(vec![ColumnWithName::new(
		ctx.fragment.clone(),
		ColumnBuffer::float8_with_bitvec(data, valids),
	)]))
}

fn execute_decimal<'a>(
	ctx: &mut FunctionContext<'a>,
	args: &Columns,
	row_count: usize,
) -> Result<Columns, RoutineError> {
	let mut sums: Vec<Decimal> = vec![Decimal::zero(); row_count];
	let mut counts = vec![0u64; row_count];
	let input_scale = args
		.iter()
		.filter_map(|col| match col.data().get_type() {
			ValueType::Decimal {
				scale,
				..
			} => Some(scale.value()),
			_ => None,
		})
		.fold(0, u8::max);
	let function = ctx.fragment.clone();
	let overflow = || avg_overflow(&function, input_scale);

	for (col_idx, col) in args.iter().enumerate() {
		let data = col.data();
		match data {
			ColumnBuffer::Int1(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Int2(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Int4(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Int8(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Int16(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Uint1(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Uint2(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Uint4(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Uint8(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ColumnBuffer::Uint16(container) => {
				let values = u128s(container);
				exec_int_arm!(values, row_count, sums, counts, overflow)
			}
			ColumnBuffer::Decimal(container) => {
				let values = decimals(container);
				for i in 0..row_count {
					if let Some(value) = values.get(i) {
						sums[i] = sums[i].checked_add(value).ok_or_else(overflow)?;
						counts[i] += 1;
					}
				}
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: col_idx,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		}
	}

	let mut out = Vec::with_capacity(row_count);
	let mut valids = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if counts[i] > 0 {
			out.push(average(&ctx.fragment, &sums[i], counts[i], input_scale)?);
			valids.push(true);
		} else {
			out.push(Decimal::zero());
			valids.push(false);
		}
	}

	Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), average_column(input_scale, out, valids))]))
}

impl Function for Avg {
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
		Ok(Some(Box::new(AvgAccumulator::new(ctx.fragment.clone()))))
	}
}

struct AvgAccumulator {
	function: Fragment,
	state: AvgState,
	counts: GroupSlots<u64>,
	input_type: Option<ValueType>,
}

enum AvgState {
	Unset,
	Int(GroupSlots<Decimal>),
	Float4(GroupSlots<f32>),
	Float8(GroupSlots<f64>),
	Decimal(GroupSlots<Decimal>),
}

impl AvgAccumulator {
	pub fn new(function: Fragment) -> Self {
		Self {
			function,
			state: AvgState::Unset,
			counts: GroupSlots::new(),
			input_type: None,
		}
	}
}

impl Accumulator for AvgAccumulator {
	fn heap_size(&self) -> usize {
		let state = match &self.state {
			AvgState::Unset => 0,
			AvgState::Int(sums) | AvgState::Decimal(sums) => sums.heap_size(),
			AvgState::Float4(sums) => sums.heap_size(),
			AvgState::Float8(sums) => sums.heap_size(),
		};
		state + self.counts.heap_size()
	}

	fn update(&mut self, args: &Columns, groups: &GroupRows) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _) = column.clone().split_nulls();
		let input_type = data.get_type();

		if self.input_type.is_none() {
			self.input_type = Some(input_type.clone());
			self.state = match input_type {
				ValueType::Float4 => AvgState::Float4(GroupSlots::new()),
				ValueType::Float8 => AvgState::Float8(GroupSlots::new()),
				ValueType::Decimal {
					..
				} => AvgState::Decimal(GroupSlots::new()),
				_ => AvgState::Int(GroupSlots::new()),
			};
		}

		let input_scale = self.input_type.as_ref().and_then(ValueType::scale).map_or(0, |scale| scale.value());
		let function = self.function.clone();
		let overflow = || avg_overflow(&function, input_scale);

		match (&mut self.state, &data) {
			(AvgState::Int(sums), ColumnBuffer::Int1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Int2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Int4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Int8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Int16(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint16(container)) => {
				let values = u128s(container);
				acc_int_arm!(sums, self.counts, column, groups, values, overflow);
			}
			(AvgState::Decimal(sums), ColumnBuffer::Decimal(container)) => {
				let values = decimals(container);
				acc_int_arm!(sums, self.counts, column, groups, values, overflow);
			}
			(AvgState::Float4(sums), ColumnBuffer::Float4(container)) => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = 0.0f32;
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.values().get(i)
						{
							delta += val;
							count += 1;
						}
					}
					if count > 0 {
						let merged = sums.remove(group).unwrap_or(0.0) + delta;
						sums.insert(group, merged);
						*self.counts.or_insert(group, 0) += count;
					} else {
						sums.or_insert(group, 0.0);
						self.counts.or_insert(group, 0);
					}
				}
			}
			(AvgState::Float8(sums), ColumnBuffer::Float8(container)) => {
				for &(group, ref indices) in groups.iter() {
					let mut delta = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.values().get(i)
						{
							delta += val;
							count += 1;
						}
					}
					if count > 0 {
						let merged = sums.remove(group).unwrap_or(0.0) + delta;
						sums.insert(group, merged);
						*self.counts.or_insert(group, 0) += count;
					} else {
						sums.or_insert(group, 0.0);
						self.counts.or_insert(group, 0);
					}
				}
			}
			(_, other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: self.function.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, ColumnBuffer), RoutineError> {
		let state = mem::replace(&mut self.state, AvgState::Unset);
		let counts = mem::take(&mut self.counts);
		let input_scale = self.input_type.as_ref().and_then(ValueType::scale).map_or(0, |scale| scale.value());

		match state {
			AvgState::Unset => {
				Ok((Vec::new(), ColumnBuilder::with_capacity(avg_decimal_type(0), 0).finish()))
			}
			AvgState::Int(sums) | AvgState::Decimal(sums) => {
				let mut keys = Vec::with_capacity(sums.len());
				let mut out = Vec::with_capacity(sums.len());
				let mut valids = Vec::with_capacity(sums.len());
				for (key, sum) in sums {
					let count = counts.get(key).copied().unwrap_or(0);
					keys.push(key);
					if count > 0 {
						out.push(average(&self.function, &sum, count, input_scale)?);
						valids.push(true);
					} else {
						out.push(Decimal::zero());
						valids.push(false);
					}
				}
				Ok((keys, average_column(input_scale, out, valids)))
			}
			AvgState::Float4(sums) => {
				let mut keys = Vec::with_capacity(sums.len());
				let mut out = Vec::with_capacity(sums.len());
				let mut valids = Vec::with_capacity(sums.len());
				for (key, sum) in sums {
					let count = counts.get(key).copied().unwrap_or(0);
					keys.push(key);
					if count > 0 {
						out.push(sum / count as f32);
						valids.push(true);
					} else {
						out.push(0.0);
						valids.push(false);
					}
				}
				Ok((keys, ColumnBuffer::float4_with_bitvec(out, valids)))
			}
			AvgState::Float8(sums) => {
				let mut keys = Vec::with_capacity(sums.len());
				let mut out = Vec::with_capacity(sums.len());
				let mut valids = Vec::with_capacity(sums.len());
				for (key, sum) in sums {
					let count = counts.get(key).copied().unwrap_or(0);
					keys.push(key);
					if count > 0 {
						out.push(sum / count as f64);
						valids.push(true);
					} else {
						out.push(0.0);
						valids.push(false);
					}
				}
				Ok((keys, ColumnBuffer::float8_with_bitvec(out, valids)))
			}
		}
	}

	fn kind_name(&self) -> &'static str {
		"math::avg"
	}
}
