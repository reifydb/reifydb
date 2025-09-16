// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Int;

use crate::value::columnar::{ColumnData, push::Push};

impl Push<Int> for ColumnData {
	fn push(&mut self, value: Int) {
		match self {
			ColumnData::Int {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::int_with_capacity(container.len());

				if let ColumnData::Int {
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
			_ => unreachable!("Push<Int> for ColumnData with incompatible type"),
		}
	}
}
