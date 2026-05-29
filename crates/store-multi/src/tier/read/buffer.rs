// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::util::cowvec::CowVec;

use crate::tier::VersionedGetResult;

#[derive(Clone)]
struct CacheEntry {
	version: CommitVersion,
	value: Option<CowVec<u8>>,
	seq: u64,
}

struct Inner {
	entries: HashMap<EncodedKey, CacheEntry>,
	next_seq: u64,
	capacity: usize,
}

#[derive(Clone)]
pub struct MultiReadBufferTier {
	inner: Arc<Mutex<Inner>>,
}

impl MultiReadBufferTier {
	pub fn new(capacity: usize) -> Self {
		Self {
			inner: Arc::new(Mutex::new(Inner {
				entries: HashMap::new(),
				next_seq: 0,
				capacity: capacity.max(1),
			})),
		}
	}

	pub fn set_capacity(&self, capacity: usize) {
		let mut inner = self.inner.lock();
		inner.capacity = capacity.max(1);
		inner.entries.clear();
	}

	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		let mut inner = self.inner.lock();
		let Some(entry) = inner.entries.get(key) else {
			return VersionedGetResult::NotFound;
		};
		if entry.version > version {
			return VersionedGetResult::NotFound;
		}
		let stored_version = entry.version;
		let value = entry.value.clone();
		let seq = inner.next_seq;
		inner.next_seq += 1;
		if let Some(e) = inner.entries.get_mut(key) {
			e.seq = seq;
		}
		match value {
			Some(v) => VersionedGetResult::Value {
				value: v,
				version: stored_version,
			},
			None => VersionedGetResult::Tombstone,
		}
	}

	pub fn insert(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		let mut inner = self.inner.lock();
		match inner.entries.get(&key) {
			Some(existing) if existing.version > version => return,
			_ => {}
		}
		let seq = inner.next_seq;
		inner.next_seq += 1;
		inner.entries.insert(
			key,
			CacheEntry {
				version,
				value,
				seq,
			},
		);
		while inner.entries.len() > inner.capacity {
			let Some(oldest) = inner.entries.iter().min_by_key(|(_, e)| e.seq).map(|(k, _)| k.clone())
			else {
				break;
			};
			inner.entries.remove(&oldest);
		}
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		self.inner.lock().entries.remove(key);
	}

	pub fn clear(&self) {
		self.inner.lock().entries.clear();
	}

	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.inner.lock().entries.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn val(s: &str) -> CowVec<u8> {
		CowVec::new(s.as_bytes().to_vec())
	}

	#[test]
	fn insert_then_get_returns_value_when_version_high_enough() {
		let read = MultiReadBufferTier::new(8);
		read.insert(key("k"), CommitVersion(5), Some(val("v5")));
		match read.get(&key("k"), CommitVersion(5)) {
			VersionedGetResult::Value {
				value: v,
				version: ver,
			} => {
				assert_eq!(v.as_ref(), b"v5");
				assert_eq!(ver, CommitVersion(5));
			}
			VersionedGetResult::Tombstone => panic!("expected value, got tombstone"),
			VersionedGetResult::NotFound => panic!("expected hit at exactly stored version"),
		}
		// A reader at a snapshot above the stored version still resolves to the cached value:
		// no newer version exists for this key, so the latest committed value is correct.
		assert!(matches!(read.get(&key("k"), CommitVersion(9)), VersionedGetResult::Value { .. }));
	}

	#[test]
	fn get_below_stored_version_misses_so_caller_reads_through() {
		// The read buffer only holds the LATEST committed value. A reader whose snapshot predates that
		// commit must NOT be served the newer value - it must fall through to the persistent tier,
		// which can resolve the correct historical version. Serving the cached value here would
		// violate snapshot isolation.
		let read = MultiReadBufferTier::new(8);
		read.insert(key("k"), CommitVersion(5), Some(val("v5")));
		assert!(
			matches!(read.get(&key("k"), CommitVersion(4)), VersionedGetResult::NotFound),
			"must miss below the stored version"
		);
	}

	#[test]
	fn tombstone_is_cached_and_served() {
		let read = MultiReadBufferTier::new(8);
		read.insert(key("k"), CommitVersion(3), None);
		assert!(matches!(read.get(&key("k"), CommitVersion(3)), VersionedGetResult::Tombstone));
	}

	#[test]
	fn invalidate_removes_the_key() {
		let read = MultiReadBufferTier::new(8);
		read.insert(key("k"), CommitVersion(1), Some(val("v1")));
		read.invalidate(&key("k"));
		assert!(
			matches!(read.get(&key("k"), CommitVersion(1)), VersionedGetResult::NotFound),
			"invalidated key must miss"
		);
	}

	#[test]
	fn newer_insert_overwrites_but_older_insert_is_ignored() {
		let read = MultiReadBufferTier::new(8);
		read.insert(key("k"), CommitVersion(5), Some(val("v5")));
		// A stale insert for an older version (e.g. a late persistent-hit populate) must not clobber
		// the newer cached value.
		read.insert(key("k"), CommitVersion(2), Some(val("v2")));
		match read.get(&key("k"), CommitVersion(5)) {
			VersionedGetResult::Value {
				value: v,
				..
			} => assert_eq!(v.as_ref(), b"v5", "older insert must not overwrite"),
			VersionedGetResult::Tombstone => panic!("unexpected tombstone"),
			VersionedGetResult::NotFound => panic!("unexpected miss"),
		}
		// A strictly newer insert replaces it.
		read.insert(key("k"), CommitVersion(7), Some(val("v7")));
		match read.get(&key("k"), CommitVersion(7)) {
			VersionedGetResult::Value {
				value: v,
				version: ver,
			} => {
				assert_eq!(v.as_ref(), b"v7");
				assert_eq!(ver, CommitVersion(7));
			}
			VersionedGetResult::Tombstone => panic!("unexpected tombstone"),
			VersionedGetResult::NotFound => panic!("unexpected miss"),
		}
	}

	#[test]
	fn eviction_bounds_size_and_never_changes_correctness() {
		// Eviction may drop any entry; a dropped entry simply forces a read-through. It must never
		// turn a hit into a wrong answer - capacity is purely a RAM/CPU trade.
		let read = MultiReadBufferTier::new(2);
		read.insert(key("a"), CommitVersion(1), Some(val("a")));
		read.insert(key("b"), CommitVersion(1), Some(val("b")));
		read.insert(key("c"), CommitVersion(1), Some(val("c")));
		assert!(read.len() <= 2, "read buffer must stay within capacity");
		// Whatever survives must still answer correctly.
		for k in ["a", "b", "c"] {
			if let VersionedGetResult::Value {
				value: v,
				..
			} = read.get(&key(k), CommitVersion(1))
			{
				assert_eq!(v.as_ref(), k.as_bytes());
			}
		}
	}

	#[test]
	fn clone_shares_backing_storage() {
		let a = MultiReadBufferTier::new(4);
		let b = a.clone();
		a.insert(key("k"), CommitVersion(1), Some(val("v")));
		assert!(
			matches!(b.get(&key("k"), CommitVersion(1)), VersionedGetResult::Value { .. }),
			"clone observes writes from the original"
		);
	}
}
