// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use reifydb_codec::key::encoded::EncodedKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortRun(EncodedKey);

impl SortRun {
	pub fn new(bytes: impl AsRef<[u8]>) -> Self {
		Self(EncodedKey::new(bytes))
	}

	pub fn from_encoded(bytes: EncodedKey) -> Self {
		Self(bytes)
	}

	pub fn as_slice(&self) -> &[u8] {
		self.0.as_slice()
	}

	pub fn len(&self) -> usize {
		self.0.as_slice().len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.as_slice().is_empty()
	}
}

impl AsRef<[u8]> for SortRun {
	fn as_ref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl Ord for SortRun {
	fn cmp(&self, other: &Self) -> Ordering {
		self.as_slice().cmp(other.as_slice())
	}
}

impl PartialOrd for SortRun {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}
