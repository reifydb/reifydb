// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::int::Int;

use crate::value::column::{ColumnBuffer, builder::ColumnBuilder, push::Push};

impl Push<Int> for ColumnBuilder {
	fn push(&mut self, value: Int) {
		match self {
			ColumnBuilder::Buffer(ColumnBuffer::Int {
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
			_ => unreachable!("Push<Int> for ColumnBuffer with incompatible type"),
		}
	}
}
