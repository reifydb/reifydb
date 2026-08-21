// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod source;

use std::sync::Arc;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::store::EntryKind, util::bloom::hash_item};
use reifydb_filter::adaptive::{AdaptiveKeyFilter, FilterMetrics};

pub const ARMED_CAPACITY_KEYS: u64 = 1_000_000;

pub(crate) fn hash_key(table: EntryKind, key: &EncodedKey) -> u64 {
	hash_item(&(table, key.as_slice()))
}

#[derive(Clone)]
pub struct MultiKeyFilter(Arc<AdaptiveKeyFilter>);

impl MultiKeyFilter {
	#[allow(clippy::new_without_default)]
	pub fn new() -> Self {
		Self(Arc::new(AdaptiveKeyFilter::new()))
	}

	pub fn armed(size_for_keys: u64) -> Self {
		Self(Arc::new(AdaptiveKeyFilter::armed(size_for_keys)))
	}

	pub fn add(&self, table: EntryKind, key: &EncodedKey) {
		self.0.add(hash_key(table, key));
	}

	pub fn may_contain(&self, table: EntryKind, key: &EncodedKey) -> bool {
		self.0.may_contain(hash_key(table, key))
	}

	pub fn metrics(&self) -> FilterMetrics {
		self.0.metrics()
	}

	pub fn handle(&self) -> Arc<AdaptiveKeyFilter> {
		self.0.clone()
	}
}
