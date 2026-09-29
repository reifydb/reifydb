// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	metrics::heap::HeapSize,
	value::column::{
		builder::ColumnBuilder,
		factory::{decimal_with_bitvec, float4_with_bitvec, float8_with_bitvec},
		nulls::split_nulls,
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
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{decimal_array::decimals, wide_int_array::wides},
		decimal::Decimal,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::function::support::{coerce::bare_type, numeric::MIN_DIVISION_SCALE};

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

fn average_column(name: &str, input_scale: u8, values: Vec<Decimal>, valids: Vec<bool>) -> (FieldRef, ArrayRef) {
	let ValueType::Decimal {
		precision,
		scale,
	} = avg_decimal_type(values.iter().map(Decimal::scale).fold(input_scale, u8::max))
	else {
		unreachable!("an average of integers or decimals is a decimal")
	};
	decimal_with_bitvec(name, precision, scale, values, valids)
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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let row_count = ctx.row_count;
		let input_type = ColumnView::try_from(&args[0])?.get_type();
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
	args: &[(FieldRef, ArrayRef)],
	row_count: usize,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let mut sums = vec![0.0f32; row_count];
	let mut counts = vec![0u32; row_count];

	for col in args.iter() {
		let data = ColumnView::try_from(col)?;
		if let ViewData::Float4(container) = &data.data {
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

	Ok(float4_with_bitvec(ctx.fragment.text(), data, valids))
}

fn execute_float8<'a>(
	ctx: &mut FunctionContext<'a>,
	args: &[(FieldRef, ArrayRef)],
	row_count: usize,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let mut sums = vec![0.0f64; row_count];
	let mut counts = vec![0u32; row_count];

	for col in args.iter() {
		let data = ColumnView::try_from(col)?;
		if let ViewData::Float8(container) = &data.data {
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

	Ok(float8_with_bitvec(ctx.fragment.text(), data, valids))
}

fn execute_decimal<'a>(
	ctx: &mut FunctionContext<'a>,
	args: &[(FieldRef, ArrayRef)],
	row_count: usize,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let mut sums: Vec<Decimal> = vec![Decimal::zero(); row_count];
	let mut counts = vec![0u64; row_count];
	let columns = args.iter().map(ColumnView::try_from).collect::<Result<Vec<_>, _>>()?;
	let input_scale = columns
		.iter()
		.filter_map(|col| match col.get_type() {
			ValueType::Decimal {
				scale,
				..
			} => Some(scale.value()),
			_ => None,
		})
		.fold(0, u8::max);
	let function = ctx.fragment.clone();
	let overflow = || avg_overflow(&function, input_scale);

	for (col_idx, data) in columns.iter().enumerate() {
		match &data.data {
			ViewData::Int1(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Int2(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Int4(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Int8(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Int16(container) => {
				let values = wides::<i128>(container);
				exec_int_arm!(values, row_count, sums, counts, overflow)
			}
			ViewData::Uint1(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Uint2(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Uint4(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Uint8(container) => {
				exec_int_arm!(container.values(), row_count, sums, counts, overflow)
			}
			ViewData::Uint16(container) => {
				let values = wides::<u128>(container);
				exec_int_arm!(values, row_count, sums, counts, overflow)
			}
			ViewData::Decimal(container) => {
				let values = decimals(container);
				for i in 0..row_count {
					if let Some(value) = values.get(i) {
						sums[i] = sums[i].checked_add(value).ok_or_else(overflow)?;
						counts[i] += 1;
					}
				}
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: col_idx,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: data.get_type(),
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

	Ok(average_column(ctx.fragment.text(), input_scale, out, valids))
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

	fn update(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError> {
		let column = ColumnView::try_from(&args[0])?;
		let (bare, _) = split_nulls(args[0].clone())?;
		let data = ColumnView::try_from(&bare)?;
		let input_type = bare_type(&data);

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

		match (&mut self.state, &data.data) {
			(AvgState::Int(sums), ViewData::Int1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Int2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Int4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Int8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Int16(container)) => {
				let values = wides::<i128>(container);
				acc_int_arm!(sums, self.counts, column, groups, values, overflow);
			}
			(AvgState::Int(sums), ViewData::Uint1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Uint2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Uint4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Uint8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container.values(), overflow);
			}
			(AvgState::Int(sums), ViewData::Uint16(container)) => {
				let values = wides::<u128>(container);
				acc_int_arm!(sums, self.counts, column, groups, values, overflow);
			}
			(AvgState::Decimal(sums), ViewData::Decimal(container)) => {
				let values = decimals(container);
				acc_int_arm!(sums, self.counts, column, groups, values, overflow);
			}
			(AvgState::Float4(sums), ViewData::Float4(container)) => {
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
			(AvgState::Float8(sums), ViewData::Float8(container)) => {
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
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: self.function.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: data.get_type(),
				});
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, (FieldRef, ArrayRef)), RoutineError> {
		let state = mem::replace(&mut self.state, AvgState::Unset);
		let counts = mem::take(&mut self.counts);
		let input_scale = self.input_type.as_ref().and_then(ValueType::scale).map_or(0, |scale| scale.value());

		match state {
			AvgState::Unset => Ok((
				Vec::new(),
				ColumnBuilder::with_capacity(avg_decimal_type(0), 0).finish(self.kind_name()),
			)),
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
				Ok((keys, average_column(self.kind_name(), input_scale, out, valids)))
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
				Ok((keys, float4_with_bitvec(self.kind_name(), out, valids)))
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
				Ok((keys, float8_with_bitvec(self.kind_name(), out, valids)))
			}
		}
	}

	fn kind_name(&self) -> &'static str {
		"math::avg"
	}
}
