// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	storage::DataBitVec,
	value::uuid::{Uuid4, Uuid7},
};

use crate::value::column::{data::ColumnData, push::Push};

impl Push<Uuid4> for ColumnData {
	fn push(&mut self, value: Uuid4) {
		match self {
			ColumnData::Uuid4(container) => container.push(value),
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => {
				panic!(
					"called `push::<Uuid4>()` on incompatible EngineColumnData::{:?}",
					other.get_type()
				);
			}
		}
	}
}

impl Push<Uuid7> for ColumnData {
	fn push(&mut self, value: Uuid7) {
		match self {
			ColumnData::Uuid7(container) => container.push(value),
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => {
				panic!(
					"called `push::<Uuid7>()` on incompatible EngineColumnData::{:?}",
					other.get_type()
				);
			}
		}
	}
}
