// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::value::encoded::key::{EncodedKey, EncodedKeyRange};

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
