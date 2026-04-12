// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Broadcasting helpers for columnar VM operations.
//!
//! When the VM operates on Columns of different lengths (e.g. a scalar constant
//! and a multi-row column), the shorter one must be broadcast to match.

use reifydb_core::value::column::{Column, data::ColumnData};

use crate::{Result, vm::vm::Vm};

impl Vm {
	/// Pop a Variable from the stack and extract its single Column.
	pub(crate) fn pop_as_column(&mut self) -> Result<Column> {
		self.stack.pop()?.into_column()
	}
}

/// If one column has length 1 and the other has length N, broadcast the short one
/// to length N by repeating its single value. If both are equal length, returns as-is.
pub(crate) fn broadcast_to_match(left: Column, right: Column) -> (Column, Column) {
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
		// Mismatched lengths that aren't broadcastable — let the kernel error
		(left, right)
	}
}

/// Repeat a single-element column to `target_len` rows.
fn broadcast_column(col: &Column, target_len: usize) -> Column {
	debug_assert_eq!(col.data.len(), 1);
	let value = col.data.get_value(0);
	let mut data = ColumnData::with_capacity(col.data.get_type(), target_len);
	for _ in 0..target_len {
		data.push_value(value.clone());
	}
	Column::new(col.name.clone(), data)
}
