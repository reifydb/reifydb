// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Clone)]
pub struct RecentCdcCache {
	inner: Arc<Mutex<BTreeMap<CommitVersion, Arc<Cdc>>>>,
	capacity: usize,
}

impl RecentCdcCache {
	pub const DEFAULT_CAPACITY: usize = 1024;

	pub fn new(capacity: usize) -> Self {
		Self {
			inner: Arc::new(Mutex::new(BTreeMap::new())),
			capacity: capacity.max(1),
		}
	}

	pub fn insert(&self, cdc: &Cdc) {
		let mut entries = self.inner.lock();
		entries.insert(cdc.version, Arc::new(cdc.clone()));
		while entries.len() > self.capacity {
			let Some(lowest) = entries.keys().next().copied() else {
				break;
			};
			entries.remove(&lowest);
		}
	}

	pub fn get(&self, version: CommitVersion) -> Option<Arc<Cdc>> {
		self.inner.lock().get(&version).cloned()
	}

	pub fn try_serve_range(
		&self,
		lo_inc: CommitVersion,
		hi_inc: CommitVersion,
		limit: usize,
	) -> Option<(Vec<Cdc>, bool)> {
		let entries = self.inner.lock();
		let min = entries.keys().next().copied()?;
		let max = entries.keys().next_back().copied()?;
		if lo_inc < min || hi_inc > max {
			return None;
		}
		let mut range = entries.range(lo_inc..=hi_inc);
		let mut items = Vec::new();
		for (_, cdc) in range.by_ref().take(limit) {
			items.push((**cdc).clone());
		}
		let has_more = range.next().is_some();
		Some((items, has_more))
	}

	pub fn clear(&self) {
		self.inner.lock().clear();
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
	fn serve_range_returns_none_when_not_fully_covered() {
		// lo below the cache's min version => caller must fall back to storage.
		let cache = RecentCdcCache::new(2);
		cache.insert(&cdc(5));
		cache.insert(&cdc(6));
		assert!(cache.try_serve_range(cv(3), cv(6), 100).is_none());
	}

	#[test]
	fn serve_range_returns_none_when_above_cache_max() {
		// hi above the cache's max version: versions in (max, hi] may exist in
		// durable storage but not in the cache, so claiming the range is empty
		// would make the caller miss them. The caller must fall back to storage.
		let cache = RecentCdcCache::new(8);
		cache.insert(&cdc(5));
		cache.insert(&cdc(6));
		assert!(cache.try_serve_range(cv(5), cv(8), 100).is_none(), "must not serve past cache max");
		assert!(cache.try_serve_range(cv(7), cv(7), 100).is_none(), "must not serve a gap above max as empty");
		let (items, _) = cache.try_serve_range(cv(5), cv(6), 100).expect("fully covered up to max");
		assert_eq!(items.len(), 2);
	}

	#[test]
	fn serve_range_returns_none_when_empty() {
		let cache = RecentCdcCache::new(4);
		assert!(cache.try_serve_range(cv(1), cv(10), 100).is_none());
	}

	#[test]
	fn serve_range_serves_covered_range_in_order() {
		let cache = RecentCdcCache::new(8);
		for v in 4..=8 {
			cache.insert(&cdc(v));
		}
		let (items, has_more) = cache.try_serve_range(cv(5), cv(7), 100).expect("covered");
		assert_eq!(items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(5), cv(6), cv(7)]);
		assert!(!has_more);
	}

	#[test]
	fn serve_range_reports_has_more_when_limited() {
		let cache = RecentCdcCache::new(8);
		for v in 1..=5 {
			cache.insert(&cdc(v));
		}
		let (items, has_more) = cache.try_serve_range(cv(1), cv(5), 2).expect("covered");
		assert_eq!(items.len(), 2);
		assert!(has_more, "more entries remain in range beyond the limit");
	}

	#[test]
	fn serve_range_at_exactly_min_is_covered() {
		let cache = RecentCdcCache::new(4);
		cache.insert(&cdc(10));
		cache.insert(&cdc(11));
		let (items, _) = cache.try_serve_range(cv(10), cv(11), 100).expect("covered at min");
		assert_eq!(items.len(), 2);
	}

	#[test]
	fn clone_shares_backing_storage() {
		let a = RecentCdcCache::new(4);
		let b = a.clone();
		a.insert(&cdc(1));
		assert!(b.get(cv(1)).is_some(), "clone observes writes from original");
	}
}
