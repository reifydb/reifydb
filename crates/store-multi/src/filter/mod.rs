// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod source;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::store::EntryKind, util::bloom::hash_item};
use reifydb_store::filter::FilterDomain;

pub const ARMED_CAPACITY_KEYS: u64 = 1_000_000;

pub struct MultiKeys;

impl FilterDomain for MultiKeys {
	type Key<'a> = (EntryKind, &'a EncodedKey);

	fn hash(key: Self::Key<'_>) -> u64 {
		hash_item(&(key.0, key.1.as_slice()))
	}
}
