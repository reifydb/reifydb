// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::int::Int};

use crate::value::column::{ColumnData, push::Push};

impl Push<Int> for ColumnData {
	fn push(&mut self, value: Int) {
		match self {
			ColumnData::Int {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			_ => unreachable!("Push<Int> for ColumnData with incompatible type"),
		}
	}
}
