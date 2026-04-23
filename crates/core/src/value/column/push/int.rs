// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::int::Int};

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
