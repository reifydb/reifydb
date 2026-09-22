// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::uint::Uint;

use crate::value::column::{ColumnBuffer, builder::ColumnBuilder, push::Push};

impl Push<Uint> for ColumnBuilder {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnBuilder::Buffer(ColumnBuffer::Uint {
				container,
				..
			}) => {
				container.push(value);
			}
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			_ => unreachable!("Push<Uint> for ColumnBuffer with incompatible type"),
		}
	}
}
