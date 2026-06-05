// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use crate::sync::rwlock::RwLock;

const MAX_SAMPLES: usize = 100_000;

#[derive(Clone)]
pub struct VersionEpoch {
	inner: Arc<RwLock<BTreeMap<u64, u64>>>,
}

impl Default for VersionEpoch {
	fn default() -> Self {
		Self::new()
	}
}

impl VersionEpoch {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	pub fn record(&self, bucket_nanos: u64, version: u64) {
		let mut samples = self.inner.write();
		if let Some((&last_bucket, &last_version)) = samples.iter().next_back() {
			if bucket_nanos < last_bucket || version < last_version {
				return;
			}
			if bucket_nanos == last_bucket {
				samples.insert(bucket_nanos, version);
				return;
			}
		}
		samples.insert(bucket_nanos, version);
		while samples.len() > MAX_SAMPLES {
			let oldest = *samples.keys().next().expect("samples is non-empty after insert");
			samples.remove(&oldest);
		}
	}

	pub fn floor_version_at(&self, target_nanos: u64) -> Option<u64> {
		self.inner.read().range(..=target_nanos).next_back().map(|(_, version)| *version)
	}

	#[cfg(test)]
	pub fn sample_count(&self) -> usize {
		self.inner.read().len()
	}
}

#[cfg(test)]
mod tests {
	use super::VersionEpoch;

	#[test]
	fn cold_epoch_returns_none_so_gc_deletes_nothing() {
		let epoch = VersionEpoch::new();
		assert_eq!(
			epoch.floor_version_at(1_000),
			None,
			"an empty epoch must yield no cutoff; otherwise a cold start would evict the whole store"
		);
	}

	#[test]
	fn floor_returns_latest_sample_at_or_before_target() {
		let epoch = VersionEpoch::new();
		epoch.record(100, 10);
		epoch.record(200, 20);
		epoch.record(300, 30);

		assert_eq!(epoch.floor_version_at(50), None, "target older than every sample -> no cutoff");
		assert_eq!(epoch.floor_version_at(100), Some(10), "exact bucket is included");
		assert_eq!(epoch.floor_version_at(250), Some(20), "floor is the latest bucket <= target");
		assert_eq!(epoch.floor_version_at(9_999), Some(30), "target after all samples -> newest");
	}

	#[test]
	fn record_drops_non_monotonic_samples() {
		let epoch = VersionEpoch::new();
		epoch.record(200, 20);
		epoch.record(100, 10);
		epoch.record(300, 15);

		assert_eq!(epoch.sample_count(), 1, "a stale bucket and a regressed version must both be rejected");
		assert_eq!(epoch.floor_version_at(9_999), Some(20));
	}

	#[test]
	fn record_keeps_highest_version_within_a_bucket() {
		// Several commits at the same wall-clock instant (e.g. a write and the flow processing it
		// triggers) must collapse to the HIGHEST version, or a row written by the later same-instant
		// commit would read as too young to ever expire.
		let epoch = VersionEpoch::new();
		epoch.record(100, 5);
		epoch.record(100, 9);
		epoch.record(100, 7);

		assert_eq!(epoch.sample_count(), 1, "one bucket holds a single sample");
		assert_eq!(epoch.floor_version_at(100), Some(9), "the highest version committed at this instant wins");
		assert_eq!(epoch.floor_version_at(9_999), Some(9));
	}
}
