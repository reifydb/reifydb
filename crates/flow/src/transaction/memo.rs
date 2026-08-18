// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::key::operator_state::Keyspace;

#[derive(Clone, Default)]
pub struct StateMemo {
	entries: Arc<DashMap<EncodedKey, Option<EncodedPodRow>>>,
	hits: Arc<AtomicU64>,
	misses: Arc<AtomicU64>,
}

impl StateMemo {
	pub fn cacheable(keyspace: Keyspace) -> bool {
		keyspace == Keyspace::JOIN_SCHEMA || keyspace == Keyspace::GROUP_DICTIONARY
	}

	pub fn lookup(&self, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		match self.entries.get(key) {
			Some(entry) => {
				self.hits.fetch_add(1, Ordering::Relaxed);
				Some(entry.value().clone())
			}
			None => {
				self.misses.fetch_add(1, Ordering::Relaxed);
				None
			}
		}
	}

	pub fn remember(&self, key: &EncodedKey, row: Option<EncodedPodRow>) {
		self.entries.insert(key.clone(), row);
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		self.entries.remove(key);
	}

	pub fn clear(&self) {
		self.entries.clear();
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn counters(&self) -> (u64, u64) {
		(self.hits.load(Ordering::Relaxed), self.misses.load(Ordering::Relaxed))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator_state::{GroupId, GroupStateKey};

	use super::*;

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn key(keyspace: Keyspace) -> EncodedKey {
		GroupStateKey::new(GroupId::ROOT, keyspace, b"suffix").into_encoded()
	}

	#[test]
	fn only_write_once_keyspaces_are_cacheable() {
		// Admitting a mutable keyspace here would serve a stale row long after a write replaced it.
		assert!(StateMemo::cacheable(Keyspace::JOIN_SCHEMA));
		assert!(StateMemo::cacheable(Keyspace::GROUP_DICTIONARY));

		for keyspace in [
			Keyspace::ACCUMULATOR,
			Keyspace::WINDOW_META,
			Keyspace::GROUP_RECORD,
			Keyspace::ROW_NUMBER_MAPPING,
			Keyspace::JOIN_RIGHT,
			Keyspace::JOIN_PUBLISHED,
			Keyspace::NODE_COUNTER,
			Keyspace::TIMER_WHEEL,
		] {
			assert!(!StateMemo::cacheable(keyspace), "{} must never be memoized", keyspace.name());
		}
	}

	#[test]
	fn a_remembered_row_is_served_back() {
		let memo = StateMemo::default();
		let k = key(Keyspace::JOIN_SCHEMA);
		memo.remember(&k, Some(row("shape")));

		assert_eq!(memo.lookup(&k), Some(Some(row("shape"))));
	}

	#[test]
	fn absence_is_remembered_as_absence_not_as_a_miss() {
		// Without a cached absence the read-before-write probe hits storage on every single row.
		let memo = StateMemo::default();
		let k = key(Keyspace::JOIN_SCHEMA);
		memo.remember(&k, None);

		assert_eq!(memo.lookup(&k), Some(None), "a cached absence must be distinguishable from an unknown key");
		assert_eq!(memo.counters().0, 1, "serving a cached absence must count as a hit");
	}

	#[test]
	fn an_unknown_key_reports_no_entry() {
		let memo = StateMemo::default();

		assert_eq!(memo.lookup(&key(Keyspace::JOIN_SCHEMA)), None);
		assert_eq!(memo.counters(), (0, 1));
	}

	#[test]
	fn invalidate_drops_only_the_named_key() {
		let memo = StateMemo::default();
		let kept = GroupStateKey::new(GroupId::ROOT, Keyspace::JOIN_SCHEMA, b"kept").into_encoded();
		let dropped = GroupStateKey::new(GroupId::ROOT, Keyspace::JOIN_SCHEMA, b"dropped").into_encoded();
		memo.remember(&kept, Some(row("a")));
		memo.remember(&dropped, Some(row("b")));

		memo.invalidate(&dropped);

		assert_eq!(memo.lookup(&dropped), None, "an invalidated key must fall through to storage");
		assert_eq!(memo.lookup(&kept), Some(Some(row("a"))), "invalidation must not evict unrelated keys");
	}

	#[test]
	fn clear_empties_every_entry() {
		let memo = StateMemo::default();
		memo.remember(&key(Keyspace::JOIN_SCHEMA), Some(row("a")));
		memo.remember(&key(Keyspace::GROUP_DICTIONARY), Some(row("b")));

		memo.clear();

		assert!(memo.is_empty(), "the batch boundary must leave nothing behind");
		assert_eq!(memo.lookup(&key(Keyspace::JOIN_SCHEMA)), None);
	}

	#[test]
	fn a_clone_shares_one_set_of_entries() {
		// A per-clone map would memoize nothing across the applies that make up one batch.
		let memo = StateMemo::default();
		let clone = memo.clone();
		clone.remember(&key(Keyspace::GROUP_DICTIONARY), Some(row("shared")));

		assert_eq!(memo.lookup(&key(Keyspace::GROUP_DICTIONARY)), Some(Some(row("shared"))));

		memo.clear();
		assert!(clone.is_empty(), "clearing one handle must clear the shared map");
	}
}
