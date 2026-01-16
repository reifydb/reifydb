// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::decimal::Decimal;

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
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::decimal_with_capacity(container.len());

				if let ColumnData::Decimal {
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
			_ => unreachable!("Push<Decimal> for ColumnData with incompatible type"),
		}
	}
}
