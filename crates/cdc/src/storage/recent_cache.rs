// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		cdc::{Cdc, SystemChange},
		change::{Change, Diff},
	},
	util::memory::{MemoryReporter, MemorySample},
};
use reifydb_runtime::sync::rwlock::RwLock;

pub enum RangeLookup {
	Hit {
		items: Vec<Cdc>,
		has_more: bool,
	},

	Overlap {
		floor: CommitVersion,
		tail: Vec<Cdc>,
		tail_has_more: bool,
	},

	Miss,
}

struct CacheInner {
	entries: BTreeMap<CommitVersion, (u64, Arc<Cdc>)>,
	bytes: u64,
}

#[derive(Clone)]
pub struct RecentCdcCache {
	inner: Arc<RwLock<CacheInner>>,
	capacity: usize,
}

impl RecentCdcCache {
	pub const DEFAULT_CAPACITY: usize = 1024;

	pub fn new(capacity: usize) -> Self {
		Self {
			inner: Arc::new(RwLock::new(CacheInner {
				entries: BTreeMap::new(),
				bytes: 0,
			})),
			capacity: capacity.max(1),
		}
	}

	pub fn insert(&self, cdc: &Cdc) {
		let bytes = cdc_bytes(cdc);
		let mut inner = self.inner.write();
		if let Some((replaced_bytes, _)) = inner.entries.insert(cdc.version, (bytes, Arc::new(cdc.clone()))) {
			inner.bytes -= replaced_bytes;
		}
		inner.bytes += bytes;
		while inner.entries.len() > self.capacity {
			let Some(lowest) = inner.entries.keys().next().copied() else {
				break;
			};
			if let Some((evicted_bytes, _)) = inner.entries.remove(&lowest) {
				inner.bytes -= evicted_bytes;
			}
		}
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	pub fn get(&self, version: CommitVersion) -> Option<Arc<Cdc>> {
		self.inner.read().entries.get(&version).map(|(_, cdc)| cdc.clone())
	}

	pub fn lookup_range(&self, lo_inc: CommitVersion, hi_inc: CommitVersion, limit: usize) -> RangeLookup {
		let inner = self.inner.read();
		let entries = &inner.entries;
		let Some(min) = entries.keys().next().copied() else {
			return RangeLookup::Miss;
		};
		let max = *entries.keys().next_back().expect("non-empty: min was found above");

		if lo_inc >= min && hi_inc <= max {
			let mut range = entries.range(lo_inc..=hi_inc);
			let items = range.by_ref().take(limit).map(|(_, (_, cdc))| (**cdc).clone()).collect();
			let has_more = range.next().is_some();
			return RangeLookup::Hit {
				items,
				has_more,
			};
		}

		if lo_inc < min && hi_inc >= min {
			let tail_hi = hi_inc.min(max);
			let mut range = entries.range(min..=tail_hi);
			let tail = range.by_ref().take(limit).map(|(_, (_, cdc))| (**cdc).clone()).collect();
			let tail_has_more = range.next().is_some();
			return RangeLookup::Overlap {
				floor: min,
				tail,
				tail_has_more,
			};
		}

		RangeLookup::Miss
	}

	pub fn clear(&self) {
		let mut inner = self.inner.write();
		inner.entries.clear();
		inner.bytes = 0;
	}

	pub fn cached_entry_count(&self) -> usize {
		self.inner.read().entries.len()
	}

	pub fn cached_entry_bytes(&self) -> u64 {
		self.inner.read().bytes
	}
}

impl MemoryReporter for RecentCdcCache {
	fn report(&self, out: &mut Vec<MemorySample>) {
		let inner = self.inner.read();
		out.push(MemorySample::new("cdc_cache", "cached_entry_count", inner.entries.len() as f64, "count"));
		out.push(MemorySample::new("cdc_cache", "cached_entry_bytes", inner.bytes as f64, "bytes"));
	}
}

fn cdc_bytes(cdc: &Cdc) -> u64 {
	let changes: usize = cdc.changes.iter().map(change_bytes).sum();
	let system: usize = cdc
		.system_changes
		.iter()
		.map(|change| size_of::<SystemChange>() + change.key().len() + change.value_bytes())
		.sum();
	(size_of::<Cdc>() + changes + system) as u64
}

fn change_bytes(change: &Change) -> usize {
	size_of::<Change>() + change.diffs.iter().map(diff_bytes).sum::<usize>()
}

fn diff_bytes(diff: &Diff) -> usize {
	size_of::<Diff>()
		+ match diff {
			Diff::Insert {
				post,
				..
			} => post.heap_size(),
			Diff::Update {
				pre,
				post,
				..
			} => pre.heap_size() + post.heap_size(),
			Diff::Remove {
				pre,
				..
			} => pre.heap_size(),
		}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

	use super::*;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc(version: u64) -> Cdc {
		Cdc::new(cv(version), DateTime::default(), Vec::new(), Vec::new())
	}

	fn cdc_with_payload(version: u64, payload: usize) -> Cdc {
		Cdc::new(
			cv(version),
			DateTime::default(),
			Vec::new(),
			vec![SystemChange::Insert {
				key: EncodedKey::new(vec![0xAB; 4]),
				post: EncodedRow(CowVec::new(vec![0u8; payload])),
			}],
		)
	}

	#[test]
	fn insert_then_get_returns_entry() {
		let cache = RecentCdcCache::new(4);
		cache.insert(&cdc(1));
		assert_eq!(cache.get(cv(1)).expect("present").version, cv(1));
		assert!(cache.get(cv(2)).is_none());
	}

	#[test]
	fn eviction_drops_lowest_version_when_over_capacity() {
		let cache = RecentCdcCache::new(2);
		cache.insert(&cdc(1));
		cache.insert(&cdc(2));
		cache.insert(&cdc(3));
		assert!(cache.get(cv(1)).is_none(), "lowest version must be evicted");
		assert!(cache.get(cv(2)).is_some());
		assert!(cache.get(cv(3)).is_some());
	}

	#[test]
	fn lookup_range_returns_overlap_when_lo_is_below_cache_floor() {
		// lo below the cache's min version, but hi reaches into the cached window:
		// the caller can serve the head from storage and the tail from the cache.
		let cache = RecentCdcCache::new(2);
		cache.insert(&cdc(5));
		cache.insert(&cdc(6));
		match cache.lookup_range(cv(3), cv(6), 100) {
			RangeLookup::Overlap {
				floor,
				tail,
				tail_has_more,
			} => {
				assert_eq!(floor, cv(5));
				assert_eq!(tail.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(5), cv(6)]);
				assert!(!tail_has_more);
			}
			_ => panic!("expected Overlap"),
		}
	}

	#[test]
	fn lookup_range_returns_miss_when_entirely_below_cache_floor() {
		// Neither lo nor hi reach the cached window at all: no data to offer.
		let cache = RecentCdcCache::new(2);
		cache.insert(&cdc(5));
		cache.insert(&cdc(6));
		assert!(matches!(cache.lookup_range(cv(1), cv(3), 100), RangeLookup::Miss));
	}

	#[test]
	fn lookup_range_returns_miss_when_above_cache_max() {
		// hi above the cache's max version: versions in (max, hi] may exist in
		// durable storage but not in the cache, so claiming the range is covered
		// would make the caller miss them. The caller must fall back to storage.
		let cache = RecentCdcCache::new(8);
		cache.insert(&cdc(5));
		cache.insert(&cdc(6));
		assert!(
			matches!(cache.lookup_range(cv(5), cv(8), 100), RangeLookup::Miss),
			"must not serve past cache max"
		);
		assert!(
			matches!(cache.lookup_range(cv(7), cv(7), 100), RangeLookup::Miss),
			"must not serve a gap above max as empty"
		);
		match cache.lookup_range(cv(5), cv(6), 100) {
			RangeLookup::Hit {
				items,
				..
			} => assert_eq!(items.len(), 2),
			_ => panic!("expected Hit for a range fully covered up to max"),
		}
	}

	#[test]
	fn lookup_range_returns_miss_when_empty() {
		let cache = RecentCdcCache::new(4);
		assert!(matches!(cache.lookup_range(cv(1), cv(10), 100), RangeLookup::Miss));
	}

	#[test]
	fn lookup_range_serves_covered_range_in_order() {
		let cache = RecentCdcCache::new(8);
		for v in 4..=8 {
			cache.insert(&cdc(v));
		}
		match cache.lookup_range(cv(5), cv(7), 100) {
			RangeLookup::Hit {
				items,
				has_more,
			} => {
				assert_eq!(
					items.iter().map(|c| c.version).collect::<Vec<_>>(),
					vec![cv(5), cv(6), cv(7)]
				);
				assert!(!has_more);
			}
			_ => panic!("expected Hit"),
		}
	}

	#[test]
	fn lookup_range_reports_has_more_when_limited() {
		let cache = RecentCdcCache::new(8);
		for v in 1..=5 {
			cache.insert(&cdc(v));
		}
		match cache.lookup_range(cv(1), cv(5), 2) {
			RangeLookup::Hit {
				items,
				has_more,
			} => {
				assert_eq!(items.len(), 2);
				assert!(has_more, "more entries remain in range beyond the limit");
			}
			_ => panic!("expected Hit"),
		}
	}

	#[test]
	fn lookup_range_at_exactly_floor_is_covered() {
		let cache = RecentCdcCache::new(4);
		cache.insert(&cdc(10));
		cache.insert(&cdc(11));
		match cache.lookup_range(cv(10), cv(11), 100) {
			RangeLookup::Hit {
				items,
				..
			} => assert_eq!(items.len(), 2),
			_ => panic!("expected Hit"),
		}
	}

	#[test]
	fn clone_shares_backing_storage() {
		let a = RecentCdcCache::new(4);
		let b = a.clone();
		a.insert(&cdc(1));
		assert!(b.get(cv(1)).is_some(), "clone observes writes from original");
	}

	#[test]
	fn byte_accounting_tracks_insert_replace_evict_and_clear() {
		// The cdc_cache memory report is only trustworthy if every mutation path keeps the byte
		// counter in exact balance: inserts add, same-version replacement swaps (no leak, no
		// double count), capacity eviction subtracts the evicted entry, and clear zeroes it.
		let cache = RecentCdcCache::new(2);
		assert_eq!(cache.cached_entry_bytes(), 0);
		assert_eq!(cache.cached_entry_count(), 0);

		cache.insert(&cdc_with_payload(1, 100));
		let one = cache.cached_entry_bytes();
		assert!(one >= 100, "the 100-byte row payload must be counted, got {one}");

		cache.insert(&cdc_with_payload(2, 100));
		assert_eq!(cache.cached_entry_bytes(), 2 * one, "two identically shaped entries must tally double");

		cache.insert(&cdc_with_payload(2, 300));
		assert_eq!(
			cache.cached_entry_bytes(),
			one + (one + 200),
			"replacing version 2 with a 200-bytes-larger payload must swap its tally, not add to it"
		);

		cache.insert(&cdc_with_payload(3, 100));
		assert_eq!(cache.cached_entry_count(), 2, "capacity 2 must evict the lowest version");
		assert!(cache.get(cv(1)).is_none());
		assert_eq!(
			cache.cached_entry_bytes(),
			(one + 200) + one,
			"the evicted entry's bytes must be released when capacity eviction removes it"
		);

		cache.clear();
		assert_eq!(cache.cached_entry_bytes(), 0, "clear must zero the byte tally");
		assert_eq!(cache.cached_entry_count(), 0);
	}

	#[test]
	fn memory_reporter_publishes_live_count_and_bytes_under_cdc_cache_scope() {
		let cache = RecentCdcCache::new(4);
		cache.insert(&cdc_with_payload(1, 64));
		cache.insert(&cdc_with_payload(2, 64));

		let mut out = Vec::new();
		cache.report(&mut out);

		let value = |metric: &str| {
			out.iter()
				.find(|s| s.scope == "cdc_cache" && s.metric == metric)
				.map(|s| s.value)
				.unwrap_or_else(|| panic!("sample cdc_cache/{metric} must be reported"))
		};
		assert_eq!(value("cached_entry_count"), 2.0);
		assert_eq!(
			value("cached_entry_bytes"),
			cache.cached_entry_bytes() as f64,
			"the reported bytes must equal the live accessor"
		);
		assert!(value("cached_entry_bytes") >= 128.0, "both payloads must be included");
	}
}
