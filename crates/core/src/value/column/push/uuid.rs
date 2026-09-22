// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	identity::IdentityId,
	uuid::{Uuid4, Uuid7},
};

use crate::value::column::{builder::ColumnBuilder, push::Push};

impl Push<Uuid4> for ColumnBuilder {
	fn push(&mut self, value: Uuid4) {
		match self {
			ColumnBuilder::Uuid4(buffer) => buffer.extend_from_slice(value.as_bytes()),
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => {
				panic!("called `push::<Uuid4>()` on incompatible ColumnBuffer::{:?}", other.get_type());
			}
		}
	}
}

impl Push<Uuid7> for ColumnBuilder {
	fn push(&mut self, value: Uuid7) {
		match self {
			ColumnBuilder::Uuid7(buffer) => buffer.extend_from_slice(value.as_bytes()),
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => {
				panic!("called `push::<Uuid7>()` on incompatible ColumnBuffer::{:?}", other.get_type());
			}
		}
	}
}

impl Push<IdentityId> for ColumnBuilder {
	fn push(&mut self, value: IdentityId) {
		match self {
			ColumnBuilder::IdentityId(buffer) => buffer.extend_from_slice(value.as_bytes()),
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => {
				panic!(
					"called `push::<IdentityId>()` on incompatible ColumnBuffer::{:?}",
					other.get_type()
				);
			}
		}
	}
}
