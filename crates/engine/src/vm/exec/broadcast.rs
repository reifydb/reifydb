// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn pop_as_column(&mut self) -> Result<ColumnWithName> {
		self.stack.pop()?.into_column()
	}
}

pub(crate) fn broadcast_to_match(left: ColumnWithName, right: ColumnWithName) -> (ColumnWithName, ColumnWithName) {
	let ll = left.data.len();
	let rl = right.data.len();

	if ll == rl {
		return (left, right);
	}

	if ll == 1 && rl > 1 {
		(broadcast_column(&left, rl), right)
	} else if rl == 1 && ll > 1 {
		(left, broadcast_column(&right, ll))
	} else {
		(left, right)
	}
}

pub(crate) fn broadcast_column(col: &ColumnWithName, target_len: usize) -> ColumnWithName {
	debug_assert_eq!(col.data.len(), 1);
	let value = col.data.get_value(0);
	let mut data = ColumnBuffer::with_capacity(col.data.get_type(), target_len);
	for _ in 0..target_len {
		data.push_value(value.clone());
	}
	ColumnWithName::new(col.name.clone(), data)
}

pub(crate) fn broadcast_many(cols: Vec<ColumnWithName>) -> Vec<ColumnWithName> {
	let target = cols.iter().map(|c| c.data.len()).max().unwrap_or(0);
	if target <= 1 {
		return cols;
	}
	cols.into_iter()
		.map(|c| {
			if c.data.len() == 1 {
				broadcast_column(&c, target)
			} else {
				c
			}
		})
		.collect()
}
