// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table storage for memory backend.

use std::{collections::BTreeMap, sync::Arc};

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::tier::EntryKind;

/// Type alias for the inner table storage (ordered key-value map)
pub(super) type Inner = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// Type alias for a table entry with async-compatible lock
pub(super) type Entry = Arc<RwLock<Inner>>;

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
