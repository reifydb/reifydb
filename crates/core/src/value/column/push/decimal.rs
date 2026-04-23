// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::decimal::Decimal};

use crate::value::column::{ColumnBuffer, push::Push};

impl Push<Decimal> for ColumnBuffer {
	fn push(&mut self, value: Decimal) {
		match self {
			ColumnBuffer::Decimal {
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
			_ => unreachable!("Push<Decimal> for ColumnBuffer with incompatible type"),
		}
	}
}
