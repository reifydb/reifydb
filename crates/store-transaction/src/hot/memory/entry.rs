// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use reifydb_type::CowVec;

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
		EntryKind::Single => "single".to_string(),
		EntryKind::Cdc => "cdc".to_string(),
		EntryKind::Source(id) => format!("source:{}", id),
		EntryKind::Operator(id) => format!("operator:{}", id),
	}
}

/// Table storage using DashMap for concurrent per-table access
pub(super) struct Entries {
	pub(super) data: DashMap<String, Entry>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: DashMap::new(),
		}
	}
}
