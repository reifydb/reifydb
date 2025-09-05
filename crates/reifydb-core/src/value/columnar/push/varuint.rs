// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Value, VarUint};

use crate::value::columnar::{ColumnData, push::Push};

impl Push<VarUint> for ColumnData {
	fn push(&mut self, value: VarUint) {
		match self {
			ColumnData::VarUint(container) => {
				container.push(Value::VarUint(value));
			}
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::varuint_with_capacity(
						container.len(),
					);

				if let ColumnData::VarUint(new_container) =
					&mut new_container
				{
					for _ in 0..container.len() {
						new_container.push_undefined();
					}
					new_container
						.push(Value::VarUint(value));
				}
				*self = new_container;
			}
			_ => unreachable!(
				"Push<VarUint> for ColumnData with incompatible type"
			),
		}
	}
}
