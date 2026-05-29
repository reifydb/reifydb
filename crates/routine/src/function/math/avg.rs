// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::mem;

use indexmap::IndexMap;
use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		decimal::Decimal,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::routine::{
	Accumulator, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};

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

fn avg_return_type(input_type: &ValueType) -> ValueType {
	match input_type {
		ValueType::Float4 => ValueType::Float4,
		ValueType::Float8 => ValueType::Float8,
		_ => ValueType::Decimal,
	}
}

macro_rules! exec_int_arm {
	($container:expr, $row_count:expr, $sums:expr, $counts:expr) => {
		for i in 0..$row_count {
			if let Some(value) = $container.get(i) {
				$sums[i] = &$sums[i] + &Decimal::from(*value);
				$counts[i] += 1;
			}
		}
	};
}

macro_rules! acc_int_arm {
	($sums:expr, $counts:expr, $column:expr, $groups:expr, $container:expr) => {
		for (group, indices) in $groups.iter() {
			let mut delta = Decimal::zero();
			let mut count = 0u64;
			for &i in indices {
				if $column.is_defined(i)
					&& let Some(&val) = $container.get(i)
				{
					delta = &delta + &Decimal::from(val);
					count += 1;
				}
			}
			if count > 0 {
				let merged = match $sums.swap_remove(group) {
					Some(prev) => &prev + &delta,
					None => delta,
				};
				$sums.insert(group.clone(), merged);
				*$counts.entry(group.clone()).or_insert(0) += count;
			} else {
				$sums.entry(group.clone()).or_insert(Decimal::zero());
				$counts.entry(group.clone()).or_insert(0);
			}
		}
	};
}

impl<'a> Routine<FunctionContext<'a>> for Avg {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().map(avg_return_type).unwrap_or(ValueType::Decimal)
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
		let (data, _bitvec) = col.data().unwrap_option();
		if let ColumnBuffer::Float4(container) = data {
			for i in 0..row_count {
				if let Some(value) = container.get(i) {
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
		let (data, _bitvec) = col.data().unwrap_option();
		if let ColumnBuffer::Float8(container) = data {
			for i in 0..row_count {
				if let Some(value) = container.get(i) {
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

	for (col_idx, col) in args.iter().enumerate() {
		let (data, _bitvec) = col.data().unwrap_option();
		match data {
			ColumnBuffer::Int1(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Int2(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Int4(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Int8(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Int16(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Uint1(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Uint2(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Uint4(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Uint8(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Uint16(container) => exec_int_arm!(container, row_count, sums, counts),
			ColumnBuffer::Int {
				container,
				..
			} => {
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						sums[i] = &sums[i] + &Decimal::from(value.clone());
						counts[i] += 1;
					}
				}
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						sums[i] = &sums[i] + &Decimal::from(value.clone());
						counts[i] += 1;
					}
				}
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						sums[i] = &sums[i] + value;
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
			let divisor = Decimal::from(counts[i] as i64);
			out.push(&sums[i] / &divisor);
			valids.push(true);
		} else {
			out.push(Decimal::zero());
			valids.push(false);
		}
	}

	Ok(Columns::new(vec![ColumnWithName::new(
		ctx.fragment.clone(),
		ColumnBuffer::decimal_with_bitvec(out, valids),
	)]))
}

impl Function for Avg {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn accumulator(&self, _ctx: &mut FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(AvgAccumulator::new()))
	}
}

struct AvgAccumulator {
	state: AvgState,
	counts: IndexMap<GroupKey, u64>,
	input_type: Option<ValueType>,
}

enum AvgState {
	Unset,
	Int(IndexMap<GroupKey, Decimal>),
	Float4(IndexMap<GroupKey, f32>),
	Float8(IndexMap<GroupKey, f64>),
	Decimal(IndexMap<GroupKey, Decimal>),
}

impl AvgAccumulator {
	pub fn new() -> Self {
		Self {
			state: AvgState::Unset,
			counts: IndexMap::new(),
			input_type: None,
		}
	}
}

impl Accumulator for AvgAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _bitvec) = column.unwrap_option();
		let input_type = data.get_type();

		if self.input_type.is_none() {
			self.input_type = Some(input_type.clone());
			self.state = match input_type {
				ValueType::Float4 => AvgState::Float4(IndexMap::new()),
				ValueType::Float8 => AvgState::Float8(IndexMap::new()),
				ValueType::Decimal => AvgState::Decimal(IndexMap::new()),
				_ => AvgState::Int(IndexMap::new()),
			};
		}

		match (&mut self.state, data) {
			(AvgState::Int(sums), ColumnBuffer::Int1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Int2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Int4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Int8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Int16(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint1(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint2(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint4(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint8(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(AvgState::Int(sums), ColumnBuffer::Uint16(container)) => {
				acc_int_arm!(sums, self.counts, column, groups, container);
			}
			(
				AvgState::Int(sums),
				ColumnBuffer::Int {
					container,
					..
				},
			) => {
				for (group, indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = &delta + &Decimal::from(val.clone());
							count += 1;
						}
					}
					if count > 0 {
						let merged = match sums.swap_remove(group) {
							Some(prev) => &prev + &delta,
							None => delta,
						};
						sums.insert(group.clone(), merged);
						*self.counts.entry(group.clone()).or_insert(0) += count;
					} else {
						sums.entry(group.clone()).or_insert(Decimal::zero());
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
			}
			(
				AvgState::Int(sums),
				ColumnBuffer::Uint {
					container,
					..
				},
			) => {
				for (group, indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = &delta + &Decimal::from(val.clone());
							count += 1;
						}
					}
					if count > 0 {
						let merged = match sums.swap_remove(group) {
							Some(prev) => &prev + &delta,
							None => delta,
						};
						sums.insert(group.clone(), merged);
						*self.counts.entry(group.clone()).or_insert(0) += count;
					} else {
						sums.entry(group.clone()).or_insert(Decimal::zero());
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
			}
			(
				AvgState::Decimal(sums),
				ColumnBuffer::Decimal {
					container,
					..
				},
			) => {
				for (group, indices) in groups.iter() {
					let mut delta = Decimal::zero();
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(val) = container.get(i)
						{
							delta = &delta + val;
							count += 1;
						}
					}
					if count > 0 {
						let merged = match sums.swap_remove(group) {
							Some(prev) => &prev + &delta,
							None => delta,
						};
						sums.insert(group.clone(), merged);
						*self.counts.entry(group.clone()).or_insert(0) += count;
					} else {
						sums.entry(group.clone()).or_insert(Decimal::zero());
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
			}
			(AvgState::Float4(sums), ColumnBuffer::Float4(container)) => {
				for (group, indices) in groups.iter() {
					let mut delta = 0.0f32;
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							delta += val;
							count += 1;
						}
					}
					if count > 0 {
						let merged = sums.swap_remove(group).unwrap_or(0.0) + delta;
						sums.insert(group.clone(), merged);
						*self.counts.entry(group.clone()).or_insert(0) += count;
					} else {
						sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
			}
			(AvgState::Float8(sums), ColumnBuffer::Float8(container)) => {
				for (group, indices) in groups.iter() {
					let mut delta = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.is_defined(i)
							&& let Some(&val) = container.get(i)
						{
							delta += val;
							count += 1;
						}
					}
					if count > 0 {
						let merged = sums.swap_remove(group).unwrap_or(0.0) + delta;
						sums.insert(group.clone(), merged);
						*self.counts.entry(group.clone()).or_insert(0) += count;
					} else {
						sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
			}
			(_, other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: Fragment::internal("math::avg"),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError> {
		let state = mem::replace(&mut self.state, AvgState::Unset);
		let counts = mem::take(&mut self.counts);

		match state {
			AvgState::Unset => Ok((Vec::new(), ColumnBuffer::with_capacity(ValueType::Decimal, 0))),
			AvgState::Int(sums) | AvgState::Decimal(sums) => {
				let mut keys = Vec::with_capacity(sums.len());
				let mut out = Vec::with_capacity(sums.len());
				let mut valids = Vec::with_capacity(sums.len());
				for (key, sum) in sums {
					let count = counts.get(&key).copied().unwrap_or(0);
					keys.push(key);
					if count > 0 {
						let divisor = Decimal::from(count as i64);
						out.push(&sum / &divisor);
						valids.push(true);
					} else {
						out.push(Decimal::zero());
						valids.push(false);
					}
				}
				Ok((keys, ColumnBuffer::decimal_with_bitvec(out, valids)))
			}
			AvgState::Float4(sums) => {
				let mut keys = Vec::with_capacity(sums.len());
				let mut out = Vec::with_capacity(sums.len());
				let mut valids = Vec::with_capacity(sums.len());
				for (key, sum) in sums {
					let count = counts.get(&key).copied().unwrap_or(0);
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
					let count = counts.get(&key).copied().unwrap_or(0);
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
