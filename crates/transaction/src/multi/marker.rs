// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{any::TaggedKey, bound::TaggedKeyBoundRange},
};
use reifydb_value::value::partition::Partition;

use crate::multi::conflict::ConflictManager;

pub struct Marker<'a> {
	marker: &'a mut ConflictManager,
}

impl<'a> Marker<'a> {
	pub fn new(marker: &'a mut ConflictManager) -> Self {
		Self {
			marker,
		}
	}

	pub fn mark(&mut self, k: &TaggedKey) {
		self.marker.mark_read(k);
	}

	pub fn mark_range(&mut self, range: TaggedKeyBoundRange) {
		self.marker.mark_range(range);
	}

	pub fn mark_member(&mut self, storage: StorageId, partition: Partition) {
		self.marker.mark_member_read(storage, partition);
	}
}
