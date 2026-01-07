// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;

use crate::tier::EntryKind;

/// Type alias for the inner table storage (lock-free ordered key-value map)
pub(super) type Inner = SkipMap<Vec<u8>, Option<Vec<u8>>>;

/// Type alias for a table entry (lock-free, no RwLock needed)
pub(super) type Entry = Arc<Inner>;

/// Convert Store to a unique string key for storage in DashMap
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
