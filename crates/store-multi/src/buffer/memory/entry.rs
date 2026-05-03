// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cmp::Reverse, collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_runtime::sync::{map::Map, rwlock::RwLock};
use reifydb_type::util::cowvec::CowVec;

pub(super) type Value = Option<CowVec<u8>>;

pub(super) type CurrentMap = BTreeMap<CowVec<u8>, (CommitVersion, Value)>;

pub(super) type HistoricalMap = BTreeMap<CowVec<u8>, BTreeMap<Reverse<CommitVersion>, Value>>;

pub(super) struct Entry {
	pub current: Arc<RwLock<CurrentMap>>,

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

pub(super) fn entry_id_to_key(entry: EntryKind) -> String {
	match entry {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source:{}", id),
		EntryKind::Operator(id) => format!("operator:{}", id),
	}
}

pub(super) struct Entries {
	pub(super) data: Map<String, Entry>,
}

impl Default for Entries {
	fn default() -> Self {
		Self {
			data: Map::new(),
		}
	}
}
