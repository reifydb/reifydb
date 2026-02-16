// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, value::decimal::Decimal};

use crate::value::column::{ColumnData, push::Push};

impl Push<Decimal> for ColumnData {
	fn push(&mut self, value: Decimal) {
		match self {
			ColumnData::Decimal {
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
			_ => unreachable!("Push<Decimal> for ColumnData with incompatible type"),
		}
	}
}
