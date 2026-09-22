// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::bignum_array::push_uint, uint::Uint};

use crate::value::column::{builder::ColumnBuilder, push::Push};

impl Push<Uint> for ColumnBuilder {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnBuilder::Uint {
				builder,
				..
			} => {
				push_uint(builder, &value);
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
