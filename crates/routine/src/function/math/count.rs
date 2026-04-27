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
use reifydb_type::value::{
	Value,
	r#type::{Type, input_types::InputTypes},
};

use crate::routine::{
	Accumulator, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};

pub struct Count {
	info: RoutineInfo,
}

impl Default for Count {
	fn default() -> Self {
		Self::new()
	}
}

impl Count {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::count"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Count {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int8
	}

	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		// SCALAR: Horizontal Count (count of non-null arguments in each row)
		let row_count = args.row_count();
		let mut counts = vec![0i64; row_count];

		for col in args.iter() {
			for (i, count) in counts.iter_mut().enumerate().take(row_count) {
				if col.data().is_defined(i) {
					*count += 1;
				}
			}
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::int8(counts))]))
	}
}

impl Function for Count {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar, FunctionKind::Aggregate]
	}

	fn accumulator(&self, _ctx: &mut FunctionContext<'_>) -> Option<Box<dyn Accumulator>> {
		Some(Box::new(CountAccumulator::new()))
	}
}

struct CountAccumulator {
	pub counts: IndexMap<GroupKey, i64>,
}

impl CountAccumulator {
	pub fn new() -> Self {
		Self {
			counts: IndexMap::new(),
		}
	}
}

impl Accumulator for CountAccumulator {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), RoutineError> {
		let column = &args[0];
		let column_name = args.name_at(0);

		// Check if this is count(*) by examining if we have a dummy column
		let is_count_star = column_name.text() == "dummy" && matches!(column, ColumnBuffer::Int4(_));

		if is_count_star {
			for (group, indices) in groups.iter() {
				let count = indices.len() as i64;
				*self.counts.entry(group.clone()).or_insert(0) += count;
			}
		} else {
			for (group, indices) in groups.iter() {
				let count = indices.iter().filter(|&i| column.is_defined(*i)).count() as i64;
				*self.counts.entry(group.clone()).or_insert(0) += count;
			}
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnBuffer), RoutineError> {
		let mut keys = Vec::with_capacity(self.counts.len());
		let mut data = ColumnBuffer::int8_with_capacity(self.counts.len());

		for (key, count) in mem::take(&mut self.counts) {
			keys.push(key);
			data.push_value(Value::Int8(count));
		}

		Ok((keys, data))
	}
}
