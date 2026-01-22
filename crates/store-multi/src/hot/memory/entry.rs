// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{cmp::Reverse, collections::BTreeMap, sync::Arc};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::concurrent_map::ConcurrentMap;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_type::util::cowvec::CowVec;

use crate::tier::EntryKind;

/// Value with optional tombstone (None = deleted)
pub(super) type Value = Option<CowVec<u8>>;

/// Current versions: key -> (version, value)
pub(super) type CurrentMap = BTreeMap<CowVec<u8>, (CommitVersion, Value)>;

/// Historical versions: key -> (version -> value)
/// Inner BTreeMap uses Reverse<CommitVersion> for descending order
pub(super) type HistoricalMap = BTreeMap<CowVec<u8>, BTreeMap<Reverse<CommitVersion>, Value>>;

/// Table entry with split current/historical storage for MVCC.
///
/// This design optimizes for the common case of reading the latest version:
/// - `current`: Most recent version per key (fast path for normal reads)
/// - `historical`: All older versions (point-in-time queries)
pub(super) struct Entry {
	/// Most recent version for each key
	pub current: Arc<RwLock<CurrentMap>>,
	/// Historical versions (all except current)
	pub historical: Arc<RwLock<HistoricalMap>>,
}

impl Entry {
	pub fn new() -> Self {
		Self {
			current: Arc::new(RwLock::new(BTreeMap::new())),
			historical: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}
}

impl Clone for Entry {
	fn clone(&self) -> Self {
		Self {
			current: Arc::clone(&self.current),
			historical: Arc::clone(&self.historical),
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
