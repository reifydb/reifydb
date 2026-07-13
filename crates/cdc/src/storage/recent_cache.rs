// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
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

#[derive(Clone)]
pub struct RecentCdcCache {
	inner: Arc<RwLock<BTreeMap<CommitVersion, Arc<Cdc>>>>,
	capacity: usize,
}

impl RecentCdcCache {
	pub const DEFAULT_CAPACITY: usize = 1024;

	pub fn new(capacity: usize) -> Self {
		Self {
			inner: Arc::new(RwLock::new(BTreeMap::new())),
			capacity: capacity.max(1),
		}
	}

	pub fn insert(&self, cdc: &Cdc) {
		let mut entries = self.inner.write();
		entries.insert(cdc.version, Arc::new(cdc.clone()));
		while entries.len() > self.capacity {
			let Some(lowest) = entries.keys().next().copied() else {
				break;
			};
			entries.remove(&lowest);
		}
	}

	pub fn get(&self, version: CommitVersion) -> Option<Arc<Cdc>> {
		self.inner.read().get(&version).cloned()
	}

	pub fn lookup_range(&self, lo_inc: CommitVersion, hi_inc: CommitVersion, limit: usize) -> RangeLookup {
		let entries = self.inner.read();
		let Some(min) = entries.keys().next().copied() else {
			return RangeLookup::Miss;
		};
		let max = *entries.keys().next_back().expect("non-empty: min was found above");

		if lo_inc >= min && hi_inc <= max {
			let mut range = entries.range(lo_inc..=hi_inc);
			let items = range.by_ref().take(limit).map(|(_, cdc)| (**cdc).clone()).collect();
			let has_more = range.next().is_some();
			return RangeLookup::Hit {
				items,
				has_more,
			};
		}

		if lo_inc < min && hi_inc >= min {
			let tail_hi = hi_inc.min(max);
			let mut range = entries.range(min..=tail_hi);
			let tail = range.by_ref().take(limit).map(|(_, cdc)| (**cdc).clone()).collect();
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
		self.inner.write().clear();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc(version: u64) -> Cdc {
		Cdc::new(cv(version), DateTime::default(), Vec::new(), Vec::new())
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
}
