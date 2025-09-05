// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Value, VarInt};

use crate::value::columnar::{ColumnData, push::Push};

impl Push<VarInt> for ColumnData {
	fn push(&mut self, value: VarInt) {
		match self {
			ColumnData::VarInt(container) => {
				container.push(Value::VarInt(value));
			}
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::varint_with_capacity(
						container.len(),
					);

				if let ColumnData::VarInt(new_container) =
					&mut new_container
				{
					for _ in 0..container.len() {
						new_container.push_undefined();
					}
					new_container
						.push(Value::VarInt(value));
				}
				*self = new_container;
			}
			_ => unreachable!(
				"Push<VarInt> for ColumnData with incompatible type"
			),
		}
	}
}
