// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::uint::Uint};

use crate::value::column::{ColumnBuffer, push::Push};

impl Push<Uint> for ColumnBuffer {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnBuffer::Uint {
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
			_ => unreachable!("Push<Uint> for ColumnBuffer with incompatible type"),
		}
	}
}
