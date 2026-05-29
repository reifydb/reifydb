// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::{storage::DataBitVec, value::decimal::Decimal};

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
