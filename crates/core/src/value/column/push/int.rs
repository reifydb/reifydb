// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::{storage::DataBitVec, value::int::Int};

use crate::value::column::{ColumnBuffer, push::Push};

impl Push<Int> for ColumnBuffer {
	fn push(&mut self, value: Int) {
		match self {
			ColumnBuffer::Int {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			_ => unreachable!("Push<Int> for ColumnBuffer with incompatible type"),
		}
	}
}
