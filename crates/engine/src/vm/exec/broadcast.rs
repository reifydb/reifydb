// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::builder::ColumnBuilder;
use reifydb_value::{reifydb_assertions, value::column_view::ColumnView};

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn pop_as_column(&mut self) -> Result<(FieldRef, ArrayRef)> {
		self.stack.pop()?.into_column()
	}
}

pub(crate) fn broadcast_to_match(
	left: (FieldRef, ArrayRef),
	right: (FieldRef, ArrayRef),
) -> Result<((FieldRef, ArrayRef), (FieldRef, ArrayRef))> {
	let ll = left.1.len();
	let rl = right.1.len();

	if ll == rl {
		return Ok((left, right));
	}

	if ll == 1 && rl > 1 {
		Ok((broadcast_column(&left, rl)?, right))
	} else if rl == 1 && ll > 1 {
		Ok((left, broadcast_column(&right, ll)?))
	} else {
		Ok((left, right))
	}
}

pub(crate) fn broadcast_column(col: &(FieldRef, ArrayRef), target_len: usize) -> Result<(FieldRef, ArrayRef)> {
	reifydb_assertions! {
		assert_eq!(col.1.len(), 1);
	}
	let view = ColumnView::try_from(col)?;
	let value = view.get_value(0);
	let mut data = ColumnBuilder::with_capacity(view.get_type(), target_len);
	for _ in 0..target_len {
		data.push_value(value.clone());
	}
	Ok(data.finish(col.0.name()))
}

pub(crate) fn broadcast_many(cols: Vec<(FieldRef, ArrayRef)>) -> Result<Vec<(FieldRef, ArrayRef)>> {
	let target = cols.iter().map(|c| c.1.len()).max().unwrap_or(0);
	if target <= 1 {
		return Ok(cols);
	}
	cols.into_iter()
		.map(|c| {
			if c.1.len() == 1 {
				broadcast_column(&c, target)
			} else {
				Ok(c)
			}
		})
		.collect()
}
