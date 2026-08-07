// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::sync::{mutex::Mutex, rwlock::RwLock};

const DEFAULT_FINE_SAMPLES: usize = 3_600;
const DEFAULT_COARSE_BUCKET: EpochSpan = EpochSpan::new(60);
const DEFAULT_MAX_SAMPLES: usize = 100_000;

pub const BUCKET_WIDTH: EpochSpan = EpochSpan::new(1);

pub const MIN_TTL: Duration = Duration::from_seconds_const(1);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EpochSeconds(u64);

impl EpochSeconds {
	pub const fn new(seconds: u64) -> Self {
		Self(seconds)
	}

	pub const fn seconds(self) -> u64 {
		self.0
	}

	pub const fn bucket(self) -> BucketIndex {
		BucketIndex(self.0 / BUCKET_WIDTH.0)
	}

	pub const fn since(self, earlier: Self) -> EpochSpan {
		EpochSpan(self.0.saturating_sub(earlier.0))
	}

	pub const fn plus(self, span: EpochSpan) -> Self {
		Self(self.0.saturating_add(span.0))
	}

	pub const fn minus(self, span: EpochSpan) -> Self {
		Self(self.0.saturating_sub(span.0))
	}

	pub fn from_datetime(at: DateTime) -> Self {
		Self(at.to_epoch_secs().max(0) as u64)
	}

	pub fn to_datetime(self) -> DateTime {
		DateTime::from_nanos(self.0.saturating_mul(1_000_000_000))
	}

	pub const fn from_nanos(nanos: u64) -> Self {
		Self(nanos / 1_000_000_000)
	}
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EpochSpan(u64);

impl EpochSpan {
	pub const fn new(seconds: u64) -> Self {
		Self(seconds)
	}

	pub const fn seconds(self) -> u64 {
		self.0
	}

	pub const fn is_zero(self) -> bool {
		self.0 == 0
	}

	pub const fn to_duration(self) -> Duration {
		Duration::from_seconds_const(self.0 as i64)
	}
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketIndex(u64);

impl BucketIndex {
	pub const fn new(index: u64) -> Self {
		Self(index)
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub const fn end(self) -> EpochSeconds {
		EpochSeconds(self.0.saturating_add(1).saturating_mul(BUCKET_WIDTH.0))
	}
}

#[derive(Clone, Copy)]
pub struct EpochRetention {
	pub fine_samples: usize,
	pub coarse_bucket: EpochSpan,
	pub max_samples: usize,
}

impl Default for EpochRetention {
	fn default() -> Self {
		Self {
			fine_samples: DEFAULT_FINE_SAMPLES,
			coarse_bucket: DEFAULT_COARSE_BUCKET,
			max_samples: DEFAULT_MAX_SAMPLES,
		}
	}
}

impl EpochRetention {
	pub fn guaranteed_coverage(&self) -> EpochSpan {
		EpochSpan(
			(self.max_samples.saturating_sub(self.fine_samples) as u64)
				.saturating_mul(self.coarse_bucket.0),
		)
	}
}

#[derive(Clone)]
pub struct VersionEpoch {
	inner: Arc<Inner>,
}

struct Inner {
	sealed: RwLock<BTreeMap<EpochSeconds, u64>>,
	retention: RwLock<EpochRetention>,
	open: Mutex<OpenBucket>,
	floor_none_returns: AtomicU64,
}

#[derive(Default, Clone, Copy)]
struct OpenBucket {
	bucket: BucketIndex,
	max: u64,
}

impl OpenBucket {
	fn floor_at(&self, target: EpochSeconds) -> Option<u64> {
		(self.max != 0 && target >= self.bucket.end()).then_some(self.max)
	}

	fn admit(&mut self, bucket: BucketIndex, version: u64) -> Option<Self> {
		if bucket < self.bucket {
			return None;
		}
		if bucket == self.bucket {
			self.max = self.max.max(version);
			return None;
		}
		let sealed = *self;
		self.bucket = bucket;
		self.max = version;
		(sealed.max != 0).then_some(sealed)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStats {
	pub samples: usize,
	pub coverage: EpochSpan,
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
				sealed: RwLock::new(BTreeMap::new()),
				retention: RwLock::new(retention),
				open: Mutex::new(OpenBucket::default()),
				floor_none_returns: AtomicU64::new(0),
			}),
		}
	}

	pub fn retention(&self) -> EpochRetention {
		*self.inner.retention.read()
	}

	pub fn set_retention(&self, retention: EpochRetention) {
		*self.inner.retention.write() = retention;
		let mut sealed = self.inner.sealed.write();
		if sealed.len() > retention.max_samples {
			compact(&mut sealed, &retention);
		}
	}

	pub fn record(&self, now: EpochSeconds, version: u64) {
		if version == 0 {
			return;
		}
		let Some(sealed) = self.inner.open.lock().admit(now.bucket(), version) else {
			return;
		};
		self.seal(sealed.bucket.end(), sealed.max);
	}

	fn seal(&self, at: EpochSeconds, version: u64) {
		let retention = *self.inner.retention.read();
		let mut sealed = self.inner.sealed.write();
		merge_max(&mut sealed, at, version);
		if sealed.len() > retention.max_samples {
			compact(&mut sealed, &retention);
		}
	}

	pub fn backfill(&self, at: EpochSeconds, version: u64) {
		if version == 0 {
			return;
		}
		let retention = *self.inner.retention.read();
		let mut sealed = self.inner.sealed.write();
		merge_max(&mut sealed, at, version);
		if sealed.len() > retention.max_samples {
			compact(&mut sealed, &retention);
		}
	}

	pub fn floor_version_at(&self, target: EpochSeconds) -> Option<u64> {
		if let Some(version) = self.inner.open.lock().floor_at(target) {
			return Some(version);
		}
		let resolved = self.inner.sealed.read().range(..=target).next_back().map(|(_, version)| *version);
		if resolved.is_none() {
			self.inner.floor_none_returns.fetch_add(1, Ordering::Relaxed);
		}
		resolved
	}

	pub fn sample_count(&self) -> usize {
		self.inner.sealed.read().len()
	}

	pub fn stats(&self) -> EpochStats {
		let open = *self.inner.open.lock();
		let open_end = (open.max != 0).then(|| open.bucket.end());

		let sealed = self.inner.sealed.read();
		let oldest = sealed.keys().next().copied().or(open_end);
		let newest = open_end.or_else(|| sealed.keys().next_back().copied());
		let coverage = match (oldest, newest) {
			(Some(oldest), Some(newest)) => newest.since(oldest),
			_ => EpochSpan::default(),
		};

		EpochStats {
			samples: sealed.len(),
			coverage,
			floor_none_returns: self.inner.floor_none_returns.load(Ordering::Relaxed),
		}
	}
}

fn merge_max(samples: &mut BTreeMap<EpochSeconds, u64>, key: EpochSeconds, version: u64) {
	let held = samples.entry(key).or_insert(version);
	if *held < version {
		*held = version;
	}
}

fn compact(samples: &mut BTreeMap<EpochSeconds, u64>, retention: &EpochRetention) {
	if !retention.coarse_bucket.is_zero() {
		let coarse_len = samples.len().saturating_sub(retention.fine_samples);
		let mut drops = Vec::new();
		let mut kept_bucket: Option<u64> = None;
		for &key in samples.keys().take(coarse_len) {
			let bucket = key.seconds() / retention.coarse_bucket.seconds();
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
	use super::{BUCKET_WIDTH, EpochRetention, EpochSeconds, EpochSpan, MIN_TTL, VersionEpoch};

	fn sec(seconds: u64) -> EpochSeconds {
		EpochSeconds::new(seconds)
	}

	fn small() -> VersionEpoch {
		VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket: EpochSpan::new(10),
			max_samples: 50,
		})
	}

	fn record_sealed(epoch: &VersionEpoch, at: EpochSeconds, version: u64) {
		// Commits seal the previous bucket, so recording only once would assert against a
		// permanently open bucket that no floor lookup at `at` can see.
		epoch.record(at, version);
		epoch.record(at.plus(BUCKET_WIDTH), version);
	}

	#[test]
	fn cold_epoch_returns_none_so_gc_deletes_nothing() {
		let epoch = VersionEpoch::new();
		assert_eq!(
			epoch.floor_version_at(sec(1_000)),
			None,
			"an empty epoch must yield no cutoff; otherwise a cold start would evict the whole store"
		);
	}

	#[test]
	fn floor_returns_latest_sample_at_or_before_target() {
		let epoch = VersionEpoch::new();
		epoch.record(sec(10), 10);
		epoch.record(sec(20), 20);
		epoch.record(sec(30), 30);

		assert_eq!(epoch.floor_version_at(sec(5)), None, "target older than every sample -> no cutoff");
		assert_eq!(epoch.floor_version_at(sec(15)), Some(10), "floor is the newest sample at or before");
		assert_eq!(epoch.floor_version_at(sec(25)), Some(20), "floor advances with the target");
		assert_eq!(epoch.floor_version_at(sec(9_999)), Some(30), "target after all samples -> newest");
	}

	#[test]
	fn the_open_bucket_is_invisible_until_the_target_clears_it() {
		// A bucket still accepting commits cannot be attributed to an instant inside itself: a
		// commit landing later in the same bucket would then read as having happened before the
		// target, and rows younger than the TTL would be evicted on that basis.
		let epoch = VersionEpoch::new();
		epoch.record(sec(10), 42);

		assert_eq!(
			epoch.floor_version_at(sec(10)),
			None,
			"an instant inside the open bucket must not resolve to that bucket's version"
		);
		assert_eq!(
			epoch.floor_version_at(sec(10).plus(BUCKET_WIDTH)),
			Some(42),
			"once the target clears the bucket end every commit it holds is known to be older"
		);
	}

	#[test]
	fn a_stale_timestamp_never_rewrites_a_newer_bucket() {
		// Wall clocks step backwards (NTP). Accepting the older reading would move the floor
		// backwards and strand rows that were already eligible to expire.
		let epoch = VersionEpoch::new();
		record_sealed(&epoch, sec(30), 30);
		epoch.record(sec(10), 5);

		assert_eq!(
			epoch.floor_version_at(sec(40)),
			Some(30),
			"a backwards clock reading must be dropped, not applied"
		);
	}

	#[test]
	fn record_keeps_highest_version_within_a_bucket() {
		// Several commits inside one bucket (a write and the flow processing it triggers) must
		// collapse to the HIGHEST version, or a row written by the later commit would read as
		// too young to ever expire.
		let epoch = VersionEpoch::new();
		epoch.record(sec(10), 5);
		epoch.record(sec(10), 9);
		epoch.record(sec(10), 7);
		epoch.record(sec(20), 20);

		assert_eq!(
			epoch.floor_version_at(sec(15)),
			Some(9),
			"the highest version committed in the bucket wins, and a lower one cannot undo it"
		);
	}

	#[test]
	fn a_bucket_holds_one_sample_however_many_commits_land_in_it() {
		// The point of bucketing: map growth is bounded by elapsed time, not by commit rate, so
		// a write-heavy database cannot inflate the epoch.
		let epoch = VersionEpoch::new();
		for version in 1..=1_000u64 {
			epoch.record(sec(10), version);
		}
		epoch.record(sec(20), 1_001);

		assert_eq!(epoch.sample_count(), 1, "a thousand commits in one bucket must seal a single sample");
		assert_eq!(epoch.floor_version_at(sec(15)), Some(1_000), "and it must carry the highest version");
	}

	#[test]
	fn old_samples_survive_far_beyond_the_uniform_eviction_limit() {
		// Uniform drop-oldest bounds coverage at max_samples * sample_interval. A ttl longer than that
		// resolves to no cutoff, and no cutoff means the class silently reclaims nothing.
		let epoch = small();
		for i in 1..=400u64 {
			epoch.record(sec(i), i);
		}

		assert!(epoch.sample_count() <= 50, "the map must stay inside its budget");
		assert!(epoch.floor_version_at(sec(400)).is_some(), "precondition: the newest end resolves");
		assert!(
			epoch.floor_version_at(sec(20)).is_some(),
			"a target 380 samples back must still resolve; uniform eviction would have dropped it"
		);
	}

	#[test]
	fn thinning_never_reports_a_version_newer_than_the_true_floor() {
		// A cutoff that is too NEW deletes rows a reader can still resolve. Thinning must therefore only ever
		// answer with an older version, never a newer one, so coarsening over-retains instead of over-deleting.
		let exact = VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket: EpochSpan::new(10),
			max_samples: 10_000,
		});
		let thinned = small();
		for i in 1..=400u64 {
			exact.record(sec(i), i);
			thinned.record(sec(i), i);
		}

		for target in (1..=400u64).map(sec) {
			let truth = exact.floor_version_at(target);
			if let Some(version) = thinned.floor_version_at(target) {
				assert!(
					Some(version) <= truth,
					"thinned floor {version:?} exceeded the true floor {truth:?} at {target:?}"
				);
			}
		}
	}

	#[test]
	fn the_newest_samples_keep_full_resolution() {
		// Short TTLs need a precise cutoff; coarsening the recent end would over-retain rows whose whole
		// purpose is to expire quickly. Precision at the recent end is one bucket, not one coarse bucket.
		let epoch = small();
		for i in 1..=400u64 {
			epoch.record(sec(i), i);
		}

		assert_eq!(epoch.floor_version_at(sec(400)), Some(399), "the newest sealed sample is exact");
		assert_eq!(epoch.floor_version_at(sec(399)), Some(398), "one second back is still exact");
	}

	#[test]
	fn a_narrowed_retention_compacts_the_existing_map_immediately() {
		// Boot replaces the constructed default with the configured rule after hydration has already filled the
		// map. Applying it lazily would leave the map over budget until the next sample, which on an idle
		// database is unbounded.
		let epoch = VersionEpoch::with_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket: EpochSpan::new(10),
			max_samples: 10_000,
		});
		for i in 1..=400u64 {
			epoch.record(sec(i), i);
		}
		assert_eq!(epoch.sample_count(), 399, "precondition: nothing compacted under the wide rule");

		epoch.set_retention(EpochRetention {
			fine_samples: 5,
			coarse_bucket: EpochSpan::new(10),
			max_samples: 50,
		});

		assert!(epoch.sample_count() <= 50, "the narrowed budget must apply to samples already held");
		assert!(epoch.floor_version_at(sec(400)).is_some(), "compaction must not empty the map");
	}

	#[test]
	fn an_unresolvable_floor_is_counted() {
		// A ttl the epoch cannot answer reclaims nothing and reports success. This counter is the only direct
		// evidence that a TTL is silently not firing.
		let epoch = VersionEpoch::new();
		epoch.record(sec(100), 1);

		assert_eq!(epoch.stats().floor_none_returns, 0, "a resolvable floor must not count");
		epoch.floor_version_at(sec(50));
		epoch.floor_version_at(sec(50));

		assert_eq!(epoch.stats().floor_none_returns, 2, "every unanswerable lookup must be counted");
	}

	#[test]
	fn coverage_reports_the_span_the_map_can_answer() {
		let epoch = VersionEpoch::new();
		assert_eq!(epoch.stats().coverage, EpochSpan::new(0), "an empty epoch covers nothing");

		epoch.record(sec(10), 1);
		epoch.record(sec(70), 2);

		assert_eq!(epoch.stats().coverage, EpochSpan::new(60), "coverage spans oldest sealed to open bucket");
		assert_eq!(epoch.stats().samples, 1, "only the rolled-over bucket is sealed; the newest is still open");
	}

	#[test]
	fn guaranteed_coverage_exceeds_the_default_retention_horizon_floor() {
		// The horizon floor promises 7 days of enforceable ttl. Coverage below it means the promise is a
		// silent no-op for every ttl in between.
		let week = EpochSpan::new(7 * 24 * 60 * 60);

		assert!(
			EpochRetention::default().guaranteed_coverage() >= week,
			"default epoch coverage must reach the default MaxRetentionHorizonFloor"
		);
	}

	#[test]
	fn the_minimum_ttl_covers_at_least_one_whole_bucket() {
		// Expiry resolves through whole buckets, so a real lifetime lands in [ttl, ttl + BUCKET_WIDTH]:
		// never early, only late. A minimum TTL covering one bucket caps that error at 100% of the
		// declared TTL, which is why the two constants may only move together.
		assert!(
			MIN_TTL.to_std().as_secs() >= BUCKET_WIDTH.seconds(),
			"a TTL at the minimum must span at least one whole bucket, or a row outlives its ttl by a \
			 multiple of itself"
		);
	}
}
