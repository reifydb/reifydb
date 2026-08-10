// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::operator_state;

use crate::metrics::heap::HeapSize;

#[operator_state]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupRecord {
	pub group: Vec<u8>,
}

impl GroupRecord {
	pub fn new(group: impl Into<Vec<u8>>) -> Self {
		Self {
			group: group.into(),
		}
	}
}

impl HeapSize for GroupRecord {
	fn heap_size(&self) -> usize {
		self.group.capacity()
	}
}
