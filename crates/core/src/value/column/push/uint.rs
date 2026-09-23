// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{decimal::Decimal, uint::Uint};

use crate::value::column::{builder::ColumnBuilder, push::Push};

impl Push<Uint> for ColumnBuilder {
	fn push(&mut self, value: Uint) {
		match self {
			ColumnBuilder::Uint(builder) => builder.push(&Decimal::from(value)),
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
