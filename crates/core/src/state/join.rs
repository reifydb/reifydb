// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_value::util::hash::xxh3_64;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentVersion(pub u64);

impl ContentVersion {
	pub fn of(encoded: &EncodedBytes) -> Self {
		Self(xxh3_64(&encoded.0).0)
	}
}
