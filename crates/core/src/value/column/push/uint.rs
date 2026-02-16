// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::uint::Uint};

use crate::value::column::{ColumnData, push::Push};

impl Push<Uint> for ColumnData {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnData::Uint {
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
			_ => unreachable!("Push<Uint> for ColumnData with incompatible type"),
		}
	}
}
