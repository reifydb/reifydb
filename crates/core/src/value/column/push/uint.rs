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
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::uint_with_capacity(container.len());

				if let ColumnData::Uint {
					container: new_container,
					..
				} = &mut new_container
				{
					for _ in 0..container.len() {
						new_container.push_undefined();
					}
					new_container.push(value);
				}
				*self = new_container;
			}
			_ => unreachable!("Push<Uint> for ColumnData with incompatible type"),
		}
	}
}
