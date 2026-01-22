// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_runtime::sync::rwlock::RwLock;

use reifydb_runtime::concurrent_map::ConcurrentMap;
use reifydb_type::util::cowvec::CowVec;

use crate::tier::EntryKind;

/// Ordered map type for storing key-value pairs.
pub(super) type OrderedMap = BTreeMap<CowVec<u8>, Option<CowVec<u8>>>;

/// Table entry using RwLock<BTreeMap> for concurrent reads and writes.
pub(super) struct Entry {
	pub data: Arc<RwLock<OrderedMap>>,
}

impl Entry {
	pub fn new() -> Self {
		Self {
			data: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}
}

impl Clone for Entry {
	fn clone(&self) -> Self {
		Self {
			data: Arc::clone(&self.data),
		}
	}
}

/// Convert EntryKind to a unique string key for storage in DashMap
pub(super) fn entry_id_to_key(entry: EntryKind) -> String {
	match entry {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source:{}", id),
		EntryKind::Operator(id) => format!("operator:{}", id),
	}
}

/// Table storage using ConcurrentMap for concurrent per-table access.
///
/// Uses DashMap on native platforms and Arc<RwLock<HashMap>> on WASM.
pub(super) struct Entries {
	pub(super) data: ConcurrentMap<String, Entry>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: ConcurrentMap::new(),
		}
	}
}
