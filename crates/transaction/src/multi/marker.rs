// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::key::{any::AnyKey, bound::AnyKeyBoundRange};

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

	pub fn mark(&mut self, k: &AnyKey) {
		self.marker.mark_read(k);
	}

	pub fn mark_range(&mut self, range: AnyKeyBoundRange) {
		self.marker.mark_range(range);
	}

	pub fn mark_range_encoded(&mut self, range: EncodedKeyRange) {
		self.marker.mark_range_encoded(range);
	}
}
