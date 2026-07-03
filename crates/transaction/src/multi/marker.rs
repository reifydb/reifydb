// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};

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

	pub fn mark(&mut self, k: &EncodedKey) {
		self.marker.mark_read(k);
	}

	pub fn mark_range(&mut self, range: EncodedKeyRange) {
		self.marker.mark_range(range);
	}
}
