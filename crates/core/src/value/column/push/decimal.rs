// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::bignum_array::push_decimal, decimal::Decimal};

use crate::value::column::{builder::ColumnBuilder, push::Push};

impl Push<Decimal> for ColumnBuilder {
	fn push(&mut self, value: Decimal) {
		match self {
			ColumnBuilder::Decimal {
				builder,
				..
			} => {
				push_decimal(builder, &value);
			}
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			_ => unreachable!("Push<Decimal> for ColumnBuffer with incompatible type"),
		}
	}
}
