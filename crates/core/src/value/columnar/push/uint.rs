// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Uint;

use crate::value::columnar::{ColumnData, push::Push};

impl Push<Uint> for ColumnData {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnData::Uint(container) => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::uint_with_capacity(
						container.len(),
					);

				if let ColumnData::Uint(new_container) =
					&mut new_container
				{
					for _ in 0..container.len() {
						new_container.push_undefined();
					}
					new_container.push(value);
				}
				*self = new_container;
			}
			_ => unreachable!(
				"Push<Uint> for ColumnData with incompatible type"
			),
		}
	}
}
