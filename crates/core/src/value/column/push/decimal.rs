// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Decimal;

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
