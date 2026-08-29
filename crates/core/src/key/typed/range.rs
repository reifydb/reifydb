// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKeyRange;

use super::MultiKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange<K = MultiKey> {
	pub start: Bound<K>,
	pub end: Bound<K>,
}

impl<K> KeyRange<K> {
	pub fn new(start: Bound<K>, end: Bound<K>) -> Self {
		Self {
			start,
			end,
		}
	}
}

impl From<&EncodedKeyRange> for KeyRange<MultiKey> {
	fn from(range: &EncodedKeyRange) -> Self {
		Self {
			start: range.start.clone(),
			end: range.end.clone(),
		}
	}
}
