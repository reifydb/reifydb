// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PageState {
	pub resident_keys: AtomicU64,
	pub unpersisted_keys: AtomicU64,
	pub last_access_ns: AtomicU64,
	pub hit_count: AtomicU64,
}

impl PageState {
	pub fn record_access(&self, now_ns: u64) {
		self.hit_count.fetch_add(1, Ordering::Relaxed);
		self.last_access_ns.store(now_ns, Ordering::Relaxed);
	}

	pub fn add_committed(&self, keys: u64) {
		self.resident_keys.fetch_add(keys, Ordering::Relaxed);
		self.unpersisted_keys.fetch_add(keys, Ordering::Relaxed);
	}

	pub fn mark_persisted(&self, keys: u64) {
		let _ = self.unpersisted_keys.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
			Some(current.saturating_sub(keys))
		});
	}

	pub fn looks_evictable(&self) -> bool {
		self.unpersisted_keys.load(Ordering::Relaxed) == 0 && self.resident_keys.load(Ordering::Relaxed) > 0
	}
}

#[cfg(test)]
mod tests {
	use super::PageState;

	#[test]
	fn committed_then_persisted_returns_to_evictable() {
		let state = PageState::default();
		assert!(!state.looks_evictable(), "an empty page is not evictable");

		state.add_committed(3);
		assert_eq!(state.unpersisted_keys.load(std::sync::atomic::Ordering::Relaxed), 3);
		assert!(!state.looks_evictable(), "unpersisted keys must block eviction");

		state.mark_persisted(3);
		assert_eq!(state.unpersisted_keys.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert!(state.looks_evictable(), "fully persisted resident page is evictable");
	}

	#[test]
	fn mark_persisted_saturates_and_does_not_underflow() {
		let state = PageState::default();
		state.add_committed(1);
		state.mark_persisted(5);
		assert_eq!(state.unpersisted_keys.load(std::sync::atomic::Ordering::Relaxed), 0);
	}
}
