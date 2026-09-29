// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	identity::IdentityId,
	uuid::{Uuid4, Uuid7},
};

use crate::value::column::{
	builder::{ColumnBuilder, TypedBuilder, append_fixed},
	push::Push,
};

impl Push<Uuid4> for ColumnBuilder {
	fn push(&mut self, value: Uuid4) {
		match &mut self.inner {
			TypedBuilder::Uuid4(builder) => append_fixed(builder, value.as_bytes()),
			other => {
				panic!(
					"called `push::<Uuid4>()` on incompatible ColumnBuilder::{:?}",
					other.get_type()
				);
			}
		}
	}
}

impl Push<Uuid7> for ColumnBuilder {
	fn push(&mut self, value: Uuid7) {
		match &mut self.inner {
			TypedBuilder::Uuid7(builder) => append_fixed(builder, value.as_bytes()),
			other => {
				panic!(
					"called `push::<Uuid7>()` on incompatible ColumnBuilder::{:?}",
					other.get_type()
				);
			}
		}
	}
}

impl Push<IdentityId> for ColumnBuilder {
	fn push(&mut self, value: IdentityId) {
		match &mut self.inner {
			TypedBuilder::IdentityId(builder) => append_fixed(builder, value.as_bytes()),
			other => {
				panic!(
					"called `push::<IdentityId>()` on incompatible ColumnBuilder::{:?}",
					other.get_type()
				);
			}
		}
	}
}
