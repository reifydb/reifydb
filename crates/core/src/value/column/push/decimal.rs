// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::decimal::Decimal;

use crate::value::column::{
	builder::{ColumnBuilder, TypedBuilder},
	push::Push,
};

impl Push<Decimal> for ColumnBuilder {
	fn push(&mut self, value: Decimal) {
		match &mut self.inner {
			TypedBuilder::Decimal(builder) => builder.push(&value),
			_ => unreachable!("Push<Decimal> for ColumnBuilder with incompatible type"),
		}
	}
}
