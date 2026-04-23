// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	storage::DataBitVec,
	value::{
		identity::IdentityId,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::{buffer::ColumnBuffer, push::Push};

impl Push<Uuid4> for ColumnBuffer {
	fn push(&mut self, value: Uuid4) {
		match self {
			ColumnBuffer::Uuid4(container) => container.push(value),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => {
				panic!("called `push::<Uuid4>()` on incompatible ColumnBuffer::{:?}", other.get_type());
			}
		}
	}
}

impl Push<Uuid7> for ColumnBuffer {
	fn push(&mut self, value: Uuid7) {
		match self {
			ColumnBuffer::Uuid7(container) => container.push(value),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => {
				panic!("called `push::<Uuid7>()` on incompatible ColumnBuffer::{:?}", other.get_type());
			}
		}
	}
}

impl Push<IdentityId> for ColumnBuffer {
	fn push(&mut self, value: IdentityId) {
		match self {
			ColumnBuffer::IdentityId(container) => container.push(value),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
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
