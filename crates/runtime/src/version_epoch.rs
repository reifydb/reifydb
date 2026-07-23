// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use crate::sync::rwlock::RwLock;

const DEFAULT_FINE_SAMPLES: usize = 3_600;
const DEFAULT_COARSE_BUCKET_NANOS: u64 = 60_000_000_000;
const DEFAULT_MAX_SAMPLES: usize = 100_000;

#[derive(Clone, Copy)]
pub struct EpochRetention {
	pub fine_samples: usize,
	pub coarse_bucket_nanos: u64,
	pub max_samples: usize,
}

impl Default for EpochRetention {
	fn default() -> Self {
		Self {
			fine_samples: DEFAULT_FINE_SAMPLES,
			coarse_bucket_nanos: DEFAULT_COARSE_BUCKET_NANOS,
			max_samples: DEFAULT_MAX_SAMPLES,
		}
	}
}

impl EpochRetention {
	pub fn guaranteed_coverage_nanos(&self) -> u64 {
		(self.max_samples.saturating_sub(self.fine_samples) as u64).saturating_mul(self.coarse_bucket_nanos)
	}
}

#[derive(Clone)]
pub struct VersionEpoch {
	inner: Arc<Inner>,
}

struct Inner {
	samples: RwLock<BTreeMap<u64, u64>>,
	retention: RwLock<EpochRetention>,
	floor_none_returns: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStats {
	pub samples: usize,
	pub coverage_nanos: u64,
	pub floor_none_returns: u64,
}

impl Default for VersionEpoch {
	fn default() -> Self {
		Self::new()
	}
}

impl VersionEpoch {
	pub fn new() -> Self {
		Self::with_retention(EpochRetention::default())
	}

	pub fn with_retention(retention: EpochRetention) -> Self {
		Self {
			inner: Arc::new(Inner {
				samples: RwLock::new(BTreeMap::new()),
				retention: RwLock::new(retention),
				floor_none_returns: AtomicU64::new(0),
			}),
		}
	}

	pub fn retention(&self) -> EpochRetention {
		*self.inner.retention.read()
	}

	pub fn set_retention(&self, retention: EpochRetention) {
		*self.inner.retention.write() = retention;
		let mut samples = self.inner.samples.write();
		if samples.len() > retention.max_samples {
			compact(&mut samples, &retention);
		}
	}

	pub fn record(&self, bucket_nanos: u64, version: u64) {
		let retention = *self.inner.retention.read();
		let mut samples = self.inner.samples.write();
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
		if samples.len() > retention.max_samples {
			compact(&mut samples, &retention);
		}
	}

	pub fn backfill(&self, bucket_nanos: u64, version: u64) {
		let retention = *self.inner.retention.read();
		let mut samples = self.inner.samples.write();
		if samples.get(&bucket_nanos).is_some_and(|held| *held >= version) {
			return;
		}
		samples.insert(bucket_nanos, version);
		if samples.len() > retention.max_samples {
			compact(&mut samples, &retention);
		}
	}

	pub fn floor_version_at(&self, target_nanos: u64) -> Option<u64> {
		let resolved =
			self.inner.samples.read().range(..=target_nanos).next_back().map(|(_, version)| *version);
		if resolved.is_none() {
			self.inner.floor_none_returns.fetch_add(1, Ordering::Relaxed);
		}
		resolved
	}

	pub fn sample_count(&self) -> usize {
		self.inner.samples.read().len()
	}

	pub fn stats(&self) -> EpochStats {
		let samples = self.inner.samples.read();
		let coverage_nanos = match (samples.keys().next(), samples.keys().next_back()) {
			(Some(oldest), Some(newest)) => newest.saturating_sub(*oldest),
			_ => 0,
		};
		EpochStats {
			samples: samples.len(),
			coverage_nanos,
			floor_none_returns: self.inner.floor_none_returns.load(Ordering::Relaxed),
		}
	}
}

fn compact(samples: &mut BTreeMap<u64, u64>, retention: &EpochRetention) {
	if retention.coarse_bucket_nanos > 0 {
		let coarse_len = samples.len().saturating_sub(retention.fine_samples);
		let mut drops = Vec::new();
		let mut kept_bucket: Option<u64> = None;
		for &key in samples.keys().take(coarse_len) {
			let bucket = key / retention.coarse_bucket_nanos;
			match kept_bucket {
				Some(previous) if previous == bucket => drops.push(key),
				_ => kept_bucket = Some(bucket),
			}
		}
		for key in drops {
			samples.remove(&key);
		}
	}

	while samples.len() > retention.max_samples {
		let oldest = *samples.keys().next().expect("samples is non-empty during compaction");
		samples.remove(&oldest);
	}
}

#[cfg(test)]
mod tests {
	use super::{EpochRetention, VersionEpoch};

	const SECOND: u64 = 1_000_000_000;

	/// Coarse buckets of 10s, exact retention of the newest 5 samples, 50 samples total.
	fn small() -> VersionEpoch {
		VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket_nanos: 10 * SECOND,
			max_samples: 50,
		})
	}

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

	#[test]
	fn old_samples_survive_far_beyond_the_uniform_eviction_limit() {
		// Uniform drop-oldest bounds coverage at max_samples * sample_interval. A ttl longer than that
		// resolves to no cutoff, and no cutoff means the class silently reclaims nothing.
		let epoch = small();
		for i in 1..=400u64 {
			epoch.record(i * SECOND, i);
		}

		assert!(epoch.sample_count() <= 50, "the map must stay inside its budget");
		assert!(epoch.floor_version_at(400 * SECOND).is_some(), "precondition: the newest end resolves");
		assert!(
			epoch.floor_version_at(20 * SECOND).is_some(),
			"a target 380 samples back must still resolve; uniform eviction would have dropped it"
		);
	}

	#[test]
	fn thinning_never_reports_a_version_newer_than_the_true_floor() {
		// A cutoff that is too NEW deletes rows a reader can still resolve. Thinning must therefore only ever
		// answer with an older version, never a newer one, so coarsening over-retains instead of over-deleting.
		let exact = VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket_nanos: 10 * SECOND,
			max_samples: 10_000,
		});
		let thinned = small();
		for i in 1..=400u64 {
			exact.record(i * SECOND, i);
			thinned.record(i * SECOND, i);
		}

		for target in (1..=400u64).map(|i| i * SECOND) {
			let truth = exact.floor_version_at(target);
			if let Some(version) = thinned.floor_version_at(target) {
				assert!(
					Some(version) <= truth,
					"thinned floor {version:?} exceeded the true floor {truth:?} at {target}"
				);
			}
		}
	}

	#[test]
	fn the_newest_samples_keep_full_resolution() {
		// Short TTLs need a precise cutoff; coarsening the recent end would over-retain rows whose whole
		// purpose is to expire quickly.
		let epoch = small();
		for i in 1..=400u64 {
			epoch.record(i * SECOND, i);
		}

		assert_eq!(epoch.floor_version_at(400 * SECOND), Some(400), "the newest sample is exact");
		assert_eq!(epoch.floor_version_at(399 * SECOND), Some(399), "one second back is still exact");
	}

	#[test]
	fn a_narrowed_retention_compacts_the_existing_map_immediately() {
		// Boot replaces the constructed default with the configured rule after hydration has already filled the
		// map. Applying it lazily would leave the map over budget until the next sample, which on an idle
		// database is unbounded.
		let epoch = VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket_nanos: 10 * SECOND,
			max_samples: 10_000,
		});
		for i in 1..=400u64 {
			epoch.record(i * SECOND, i);
		}
		assert_eq!(epoch.sample_count(), 400, "precondition: nothing compacted under the wide rule");

		epoch.set_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket_nanos: 10 * SECOND,
			max_samples: 50,
		});

		assert!(epoch.sample_count() <= 50, "the narrowed budget must apply to samples already held");
		assert!(epoch.floor_version_at(400 * SECOND).is_some(), "compaction must not empty the map");
	}

	#[test]
	fn an_unresolvable_floor_is_counted() {
		// A ttl the epoch cannot answer reclaims nothing and reports success. This counter is the only direct
		// evidence that a TTL is silently not firing.
		let epoch = VersionEpoch::new();
		epoch.record(100 * SECOND, 1);

		assert_eq!(epoch.stats().floor_none_returns, 0, "a resolvable floor must not count");
		epoch.floor_version_at(50 * SECOND);
		epoch.floor_version_at(50 * SECOND);

		assert_eq!(epoch.stats().floor_none_returns, 2, "every unanswerable lookup must be counted");
	}

	#[test]
	fn coverage_reports_the_span_the_map_can_answer() {
		let epoch = VersionEpoch::new();
		assert_eq!(epoch.stats().coverage_nanos, 0, "an empty epoch covers nothing");

		epoch.record(10 * SECOND, 1);
		epoch.record(70 * SECOND, 2);

		assert_eq!(epoch.stats().coverage_nanos, 60 * SECOND, "coverage spans oldest to newest sample");
		assert_eq!(epoch.stats().samples, 2);
	}

	#[test]
	fn guaranteed_coverage_exceeds_the_default_retention_horizon_floor() {
		// The horizon floor promises 7 days of enforceable ttl. Coverage below it means the promise is a
		// silent no-op for every ttl in between.
		let week_nanos = 7 * 24 * 60 * 60 * SECOND;

		assert!(
			EpochRetention::default().guaranteed_coverage_nanos() >= week_nanos,
			"default epoch coverage must reach the default MaxRetentionHorizonFloor"
		);
	}
}
