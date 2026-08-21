// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_runtime::sync::rwlock::RwLock;

use crate::bloom::BloomFilter;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FilterMetrics {
	pub enabled: bool,
	pub rebuilding: bool,
	pub fill_ratio: f64,
	pub estimated_keys: u64,
	pub size_bits: u64,
	pub queries: u64,
	pub rejected: u64,
	pub rebuilds: u64,
	pub aborts: u64,
}

struct FilterState {
	active: Option<Arc<BloomFilter>>,
	building: Option<Arc<BloomFilter>>,
}

pub struct RebuildHandle {
	filter: Arc<BloomFilter>,
}

impl RebuildHandle {
	pub fn feed(&self, hashes: &[u64]) {
		for hash in hashes {
			self.filter.add_hash(*hash);
		}
	}
}

pub struct AdaptiveKeyFilter {
	state: RwLock<FilterState>,
	queries: AtomicU64,
	rejected: AtomicU64,
	rebuilds: AtomicU64,
	aborts: AtomicU64,
}

impl Default for AdaptiveKeyFilter {
	fn default() -> Self {
		Self::new()
	}
}

impl AdaptiveKeyFilter {
	pub fn new() -> Self {
		Self {
			state: RwLock::new(FilterState {
				active: None,
				building: None,
			}),
			queries: AtomicU64::new(0),
			rejected: AtomicU64::new(0),
			rebuilds: AtomicU64::new(0),
			aborts: AtomicU64::new(0),
		}
	}

	pub fn add(&self, hash: u64) {
		let state = self.state.read();
		if let Some(active) = &state.active {
			active.add_hash(hash);
		}
		if let Some(building) = &state.building {
			building.add_hash(hash);
		}
	}

	pub fn may_contain(&self, hash: u64) -> bool {
		self.queries.fetch_add(1, Ordering::Relaxed);
		let state = self.state.read();
		let Some(active) = &state.active else {
			return true;
		};
		if active.might_contain_hash(hash) {
			return true;
		}
		self.rejected.fetch_add(1, Ordering::Relaxed);
		false
	}

	pub fn is_enabled(&self) -> bool {
		self.state.read().active.is_some()
	}

	pub fn begin_rebuild(&self, size_for_keys: u64) -> RebuildHandle {
		let mut state = self.state.write();
		assert!(
			state.building.is_none(),
			"begin_rebuild called while a rebuild is already in flight for this filter"
		);
		let filter = Arc::new(BloomFilter::new(size_for_keys as usize));
		state.building = Some(filter.clone());
		RebuildHandle {
			filter,
		}
	}

	pub fn commit_rebuild(&self, handle: RebuildHandle) {
		let mut state = self.state.write();
		state.active = Some(handle.filter);
		state.building = None;
		self.rebuilds.fetch_add(1, Ordering::Relaxed);
	}

	pub fn abort_rebuild(&self, handle: RebuildHandle) {
		let mut state = self.state.write();
		state.building = None;
		drop(handle);
		self.aborts.fetch_add(1, Ordering::Relaxed);
	}

	pub fn metrics(&self) -> FilterMetrics {
		let state = self.state.read();
		let (enabled, fill_ratio, estimated_keys, size_bits) = match &state.active {
			Some(active) => {
				(true, active.fill_ratio(), active.estimated_items() as u64, active.size_bits() as u64)
			}
			None => (false, 0.0, 0, 0),
		};
		FilterMetrics {
			enabled,
			rebuilding: state.building.is_some(),
			fill_ratio,
			estimated_keys,
			size_bits,
			queries: self.queries.load(Ordering::Relaxed),
			rejected: self.rejected.load(Ordering::Relaxed),
			rebuilds: self.rebuilds.load(Ordering::Relaxed),
			aborts: self.aborts.load(Ordering::Relaxed),
		}
	}
}
