// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::bignum_array::push_int, int::Int};

use crate::value::column::{builder::ColumnBuilder, push::Push};

impl Push<Int> for ColumnBuilder {
	fn push(&mut self, value: Int) {
		match self {
			ColumnBuilder::Int {
				builder,
				..
			} => {
				push_int(builder, &value);
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
