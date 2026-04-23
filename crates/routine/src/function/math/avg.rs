// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use indexmap::IndexMap;
use num_traits::ToPrimitive;
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
		r#type::{Type, input_types::InputTypes},
	},
};

use crate::function::{Accumulator, Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Avg {
	info: FunctionInfo,
}

impl Default for Avg {
	fn default() -> Self {
		Self::new()
	}
}

impl Avg {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::avg"),
		}
	}
}

impl Function for Avg {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar, FunctionCapability::Aggregate]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Float8
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::numeric()
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		let row_count = args.row_count();
		let mut sum = vec![0.0f64; row_count];
		let mut count = vec![0u32; row_count];

		for (col_idx, col) in args.iter().enumerate() {
			let (data, _bitvec) = col.data().unwrap_option();
			match data {
				ColumnBuffer::Int1(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Int2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Int4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Int8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Int16(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint1(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint2(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint16(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Float4(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value as f64;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Float8(container) => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += *value;
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Int {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Uint {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				ColumnBuffer::Decimal {
					container,
					..
				} => {
					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							sum[i] += value.0.to_f64().unwrap_or(0.0);
							count[i] += 1;
						}
					}
				}
				other => {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: col_idx,
						expected: self.accepted_types().expected_at(0).to_vec(),
						actual: other.get_type(),
					});
				}
			}
		}

		let mut data = Vec::with_capacity(row_count);
		let mut valids = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if count[i] > 0 {
				data.push(sum[i] / count[i] as f64);
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

	fn accumulator(&self, _ctx: &FunctionContext) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(AvgAccumulator::new()))
	}
}

struct AvgAccumulator {
	pub sums: IndexMap<GroupKey, f64>,
	pub counts: IndexMap<GroupKey, u64>,
}

impl AvgAccumulator {
	pub fn new() -> Self {
		Self {
			sums: IndexMap::new(),
			counts: IndexMap::new(),
		}
	}
}

macro_rules! avg_arm {
	($self:expr, $column:expr, $groups:expr, $container:expr) => {
		for (group, indices) in $groups.iter() {
			let mut sum = 0.0f64;
			let mut count = 0u64;
			for &i in indices {
				if $column.data().is_defined(i) {
					if let Some(&val) = $container.get(i) {
						sum += val as f64;
						count += 1;
					}
				}
			}
			if count > 0 {
				$self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
				$self.counts.entry(group.clone()).and_modify(|c| *c += count).or_insert(count);
			} else {
				$self.sums.entry(group.clone()).or_insert(0.0);
				$self.counts.entry(group.clone()).or_insert(0);
			}
		}
	};
}

impl Accumulator for AvgAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), FunctionError> {
		let column = &args[0];
		let (data, _bitvec) = column.data().unwrap_option();

		match data {
			ColumnBuffer::Int1(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Int2(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Int4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Int8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Int16(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Uint1(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Uint2(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Uint4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Uint8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Uint16(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Float4(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Float8(container) => {
				avg_arm!(self, column, groups, container);
				Ok(())
			}
			ColumnBuffer::Int {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				for (group, indices) in groups.iter() {
					let mut sum = 0.0f64;
					let mut count = 0u64;
					for &i in indices {
						if column.data().is_defined(i)
							&& let Some(val) = container.get(i)
						{
							sum += val.0.to_f64().unwrap_or(0.0);
							count += 1;
						}
					}
					if count > 0 {
						self.sums.entry(group.clone()).and_modify(|v| *v += sum).or_insert(sum);
						self.counts
							.entry(group.clone())
							.and_modify(|c| *c += count)
							.or_insert(count);
					} else {
						self.sums.entry(group.clone()).or_insert(0.0);
						self.counts.entry(group.clone()).or_insert(0);
					}
				}
				Ok(())
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: Fragment::internal("math::avg"),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other.get_type(),
			}),
		}
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), FunctionError> {
		let mut keys = Vec::with_capacity(self.sums.len());
		let mut data = ColumnBuffer::float8_with_capacity(self.sums.len());

		for (key, sum) in mem::take(&mut self.sums) {
			let count = self.counts.swap_remove(&key).unwrap_or(0);
			keys.push(key);
			if count > 0 {
				data.push_value(Value::float8(sum / count as f64));
			} else {
				data.push_value(Value::none());
			}
		}

		Ok((keys, data))
	}
}
