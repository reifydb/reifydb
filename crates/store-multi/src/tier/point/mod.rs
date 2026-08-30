// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, default, key::typed::MultiKey};
use reifydb_store::tier::{
	point::{PointConfig, PointDomain, PointMetrics, PointTier},
	range::RowBytes,
};
use reifydb_store_commit::VersionedGetResult;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};

#[derive(Clone, Copy, Debug)]
pub struct MultiPointConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl MultiPointConfig {
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(default::store::MULTI_POINT_BUFFER_SHARD_TESTING),
			shards: default::store::MULTI_POINT_BUFFER_SHARDS_TESTING as usize,
		}
	}
}

impl From<MultiPointConfig> for PointConfig {
	fn from(config: MultiPointConfig) -> Self {
		Self {
			shard_bytes: config.shard_bytes,
			shards: config.shards,
		}
	}
}

#[derive(Clone, Debug)]
pub struct MultiPointRow {
	pub version: CommitVersion,
	pub value: Option<CowVec<u8>>,
	pub previous: Option<Box<(CommitVersion, Option<CowVec<u8>>)>>,
}

impl MultiPointRow {
	pub fn new(version: CommitVersion, value: Option<CowVec<u8>>) -> Self {
		Self {
			version,
			value,
			previous: None,
		}
	}

	pub fn at(&self, read: CommitVersion) -> Option<(CommitVersion, &Option<CowVec<u8>>)> {
		if self.version <= read {
			return Some((self.version, &self.value));
		}
		match self.previous.as_deref() {
			Some((version, value)) if *version <= read => Some((*version, value)),
			_ => None,
		}
	}

	pub fn served_previous(&self, read: CommitVersion) -> bool {
		self.version > read && self.previous.as_deref().is_some_and(|(version, _)| *version <= read)
	}
}

impl RowBytes for MultiPointRow {
	fn row_bytes(&self) -> usize {
		let current = self.value.as_ref().map_or(0, |value| value.len());
		let previous = self.previous.as_deref().map_or(0, |(_, value)| value.as_ref().map_or(0, CowVec::len));
		current + previous
	}
}

#[derive(Clone, Copy, Debug)]
pub struct MultiPointDomain;

impl PointDomain for MultiPointDomain {
	type Dimension = ();
	type Key = MultiKey;
	type MetricBucket = ();
	type Row = MultiPointRow;

	const METRIC_BUCKETS: usize = 1;

	const SCOPE: &'static str = "multi_point";

	fn metric_bucket(_key: &EncodedKey) -> Option<usize> {
		Some(0)
	}

	fn caches_points(_slot: usize) -> bool {
		true
	}

	fn supersede(resident: &mut Self::Row, incoming: Self::Row) -> bool {
		if resident.version > incoming.version {
			return false;
		}
		resident.previous = if resident.version < incoming.version {
			Some(Box::new((resident.version, resident.value.take())))
		} else {
			None
		};
		resident.version = incoming.version;
		resident.value = incoming.value;
		true
	}

	fn metric_bucket_at(_index: usize) -> Self::MetricBucket {}

	fn metric_bucket_name(_slot: Self::MetricBucket) -> Cow<'static, str> {
		Cow::Borrowed("row")
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiReadMetrics {
	pub hits: u64,
	pub previous_hits: u64,
	pub misses: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct MultiPointShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub entries: usize,
	pub counters: PointMetrics,
	pub reads: MultiReadMetrics,
}

#[derive(Default)]
struct ReadCounters {
	hits: AtomicU64,
	previous_hits: AtomicU64,
	misses: AtomicU64,
}

#[derive(Clone)]
pub struct MultiPointTier {
	tier: PointTier<MultiPointDomain>,
	reads: Arc<[ReadCounters]>,
}

impl MultiPointTier {
	pub fn new(config: MultiPointConfig) -> Option<Self> {
		let tier = PointTier::new(config.into())?;
		let shards = config.shards.max(1);
		Some(Self {
			tier,
			reads: (0..shards).map(|_| ReadCounters::default()).collect(),
		})
	}

	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> VersionedGetResult {
		let counters = &self.reads[self.tier.shard_index((), key)];
		let Some(Some(row)) = self.tier.get((), key) else {
			counters.misses.fetch_add(1, Ordering::Relaxed);
			return VersionedGetResult::NotFound;
		};
		let previous = row.served_previous(version);
		let Some((served, value)) = row.at(version) else {
			counters.misses.fetch_add(1, Ordering::Relaxed);
			return VersionedGetResult::NotFound;
		};
		let counter = if previous {
			&counters.previous_hits
		} else {
			&counters.hits
		};
		counter.fetch_add(1, Ordering::Relaxed);
		match value {
			Some(value) => VersionedGetResult::Value {
				value: value.clone(),
				version: served,
			},
			None => VersionedGetResult::Tombstone,
		}
	}

	pub fn insert(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		self.tier.overwrite((), key, MultiPointRow::new(version, value));
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		self.tier.invalidate((), key);
	}

	pub fn clear(&self) {
		self.tier.clear();
	}

	pub fn read_metrics(&self) -> Vec<MultiReadMetrics> {
		self.reads
			.iter()
			.map(|counters| MultiReadMetrics {
				hits: counters.hits.load(Ordering::Relaxed),
				previous_hits: counters.previous_hits.load(Ordering::Relaxed),
				misses: counters.misses.load(Ordering::Relaxed),
			})
			.collect()
	}

	pub fn shard_metrics(&self) -> Vec<MultiPointShardMetrics> {
		let shards = self.tier.shard_metrics();
		let reads = self.read_metrics();
		reifydb_assertions! {
			assert_eq!(
				shards.len(),
				reads.len(),
				"every shard must report both sources, or a shard past the shortest reports zero forever"
			);
		}
		shards.into_iter()
			.zip(reads)
			.map(|(shard, reads)| MultiPointShardMetrics {
				shard: shard.shard,
				used: shard.used,
				limit: shard.limit,
				entries: shard.entries,
				counters: shard.counters,
				reads,
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use std::str::from_utf8;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::catalog::{id::TableId, storage::StorageId},
		key::{EncodableKey, row::RowKey},
	};
	use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

	use super::{
		CommitVersion, CowVec, MultiPointConfig, MultiPointDomain, MultiPointRow, MultiPointTier,
		MultiReadMetrics, PointDomain, RowBytes, VersionedGetResult,
	};

	fn tier() -> MultiPointTier {
		MultiPointTier::new(MultiPointConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 4,
		})
		.expect("a configured budget yields a tier")
	}

	fn row_key(n: u64) -> EncodedKey {
		RowKey {
			storage: StorageId::Table(TableId(1)),
			row: RowNumber(n),
		}
		.encode()
	}

	fn used(tier: &MultiPointTier) -> u64 {
		tier.shard_metrics().into_iter().map(|shard| shard.used.as_bytes()).sum()
	}

	fn totals(tier: &MultiPointTier) -> MultiReadMetrics {
		tier.read_metrics().into_iter().fold(MultiReadMetrics::default(), |mut acc, shard| {
			acc.hits += shard.hits;
			acc.previous_hits += shard.previous_hits;
			acc.misses += shard.misses;
			acc
		})
	}

	fn value(body: &str) -> Option<CowVec<u8>> {
		Some(CowVec::new(body.as_bytes().to_vec()))
	}

	fn row(version: u64, body: &str) -> MultiPointRow {
		MultiPointRow::new(CommitVersion(version), value(body))
	}

	fn body(slot: &Option<CowVec<u8>>) -> &str {
		from_utf8(slot.as_ref().expect("the slot must carry a value")).expect("test bodies are utf8")
	}

	#[test]
	fn an_older_write_is_refused_rather_than_seated() {
		// Seating it would move the current slot backwards and strand the newer value in previous.
		let mut resident = row(5, "new");

		assert!(!MultiPointDomain::supersede(&mut resident, row(3, "old")), "an older write must be refused");
		assert_eq!(resident.version, CommitVersion(5), "the refusal moved the version backwards");
		assert_eq!(body(&resident.value), "new", "the refusal took the older value");
		assert!(resident.previous.is_none(), "the refusal invented a previous slot");
	}

	#[test]
	fn a_write_at_the_same_version_replaces_without_inventing_a_previous() {
		// One version can only hold one value, so keeping the displaced one would let a reader see a version
		// twice.
		let mut resident = row(5, "first");

		assert!(MultiPointDomain::supersede(&mut resident, row(5, "second")), "a same-version write must land");
		assert_eq!(body(&resident.value), "second", "the newer value at the same version never landed");
		assert!(resident.previous.is_none(), "a same-version replace fabricated a version that never existed");
	}

	#[test]
	fn a_newer_write_pushes_the_displaced_value_into_previous() {
		// Dropping it instead makes every reader below the new version fall through to persistent.
		let mut resident = row(5, "old");

		assert!(MultiPointDomain::supersede(&mut resident, row(9, "new")), "a newer write must land");
		assert_eq!(resident.version, CommitVersion(9));
		assert_eq!(body(&resident.value), "new");
		let (version, displaced) = resident.previous.as_deref().expect("the displaced value must be kept");
		assert_eq!(*version, CommitVersion(5), "previous must carry the version it was written at");
		assert_eq!(body(displaced), "old");
	}

	#[test]
	fn a_displaced_tombstone_is_kept_like_any_other_value() {
		// Dropping it would let a reader below the new version see the value the tombstone deleted.
		let mut resident = MultiPointRow::new(CommitVersion(5), None);

		assert!(MultiPointDomain::supersede(&mut resident, row(9, "resurrected")), "a newer write must land");
		let (version, displaced) = resident.previous.as_deref().expect("a displaced tombstone must be kept");
		assert_eq!(*version, CommitVersion(5));
		assert!(displaced.is_none(), "the tombstone was rewritten as a value");
	}

	#[test]
	fn a_third_write_forgets_the_oldest_of_the_three() {
		// The chain is two deep by design, so the oldest must fall off rather than grow the row without bound.
		let mut resident = row(1, "first");
		MultiPointDomain::supersede(&mut resident, row(2, "second"));
		MultiPointDomain::supersede(&mut resident, row(3, "third"));

		assert_eq!(body(&resident.value), "third");
		let (version, displaced) =
			resident.previous.as_deref().expect("the chain must still hold one displaced value");
		assert_eq!(*version, CommitVersion(2), "the chain kept the wrong version");
		assert_eq!(body(displaced), "second", "a two-deep chain must forget the oldest, not the newest");
	}

	#[test]
	fn a_reader_below_the_current_version_is_served_from_previous() {
		// Answering from the current slot would show a reader a version committed after its snapshot.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		let (version, served) =
			resident.at(CommitVersion(7)).expect("previous must answer a reader below the current version");
		assert_eq!(version, CommitVersion(5));
		assert_eq!(body(served), "old");
		assert!(
			resident.served_previous(CommitVersion(7)),
			"the read came from previous and must be counted as such"
		);
	}

	#[test]
	fn a_reader_below_both_versions_is_not_served_at_all() {
		// Serving the oldest slot anyway would answer with a value written after the reader's snapshot.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		assert!(
			resident.at(CommitVersion(4)).is_none(),
			"a reader below every cached version must fall through"
		);
		assert!(
			!resident.served_previous(CommitVersion(4)),
			"a fall-through must not be counted as a previous hit"
		);
	}

	#[test]
	fn a_reader_at_or_above_the_current_version_is_served_from_the_current_slot() {
		// Reading previous here would hand back a value the newer write already replaced.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		let (version, served) =
			resident.at(CommitVersion(9)).expect("the current slot must answer its own version");
		assert_eq!(version, CommitVersion(9));
		assert_eq!(body(served), "new");
		assert!(
			!resident.served_previous(CommitVersion(9)),
			"a current-slot read must not be counted as a previous hit"
		);
	}

	#[test]
	fn the_footprint_counts_both_slots() {
		// Counting only the current slot lets the chain grow the row while the budget reports it unchanged.
		let mut resident = row(5, "aaaa");
		let current_only = resident.row_bytes();
		MultiPointDomain::supersede(&mut resident, row(9, "bbbbbbbb"));

		assert_eq!(current_only, 4);
		assert_eq!(resident.row_bytes(), 12, "the displaced value is resident and must be charged for");
	}

	#[test]
	fn a_read_at_the_current_version_is_a_hit() {
		let tier = tier();
		let key = row_key(1);
		tier.insert(key.clone(), CommitVersion(5), value("five"));

		match tier.get(&key, CommitVersion(7)) {
			VersionedGetResult::Value {
				value,
				version,
			} => {
				assert_eq!(version, CommitVersion(5));
				assert_eq!(value.as_ref(), b"five");
			}
			other => panic!("expected the cached value, got {other:?}"),
		}
		assert_eq!(
			totals(&tier),
			MultiReadMetrics {
				hits: 1,
				previous_hits: 0,
				misses: 0
			}
		);
	}

	#[test]
	fn a_read_below_the_newest_version_is_served_from_previous_and_counted_apart() {
		let tier = tier();
		let key = row_key(2);
		tier.insert(key.clone(), CommitVersion(5), value("five"));
		tier.insert(key.clone(), CommitVersion(9), value("nine"));

		match tier.get(&key, CommitVersion(6)) {
			VersionedGetResult::Value {
				value,
				version,
			} => {
				assert_eq!(version, CommitVersion(5));
				assert_eq!(value.as_ref(), b"five");
			}
			other => panic!("expected the displaced value, got {other:?}"),
		}
		assert_eq!(
			totals(&tier),
			MultiReadMetrics {
				hits: 0,
				previous_hits: 1,
				misses: 0
			},
			"a read the second slot answered must not be indistinguishable from one the first answered"
		);
	}

	#[test]
	fn a_read_below_every_cached_version_is_a_miss_not_a_hit() {
		let tier = tier();
		let key = row_key(3);
		tier.insert(key.clone(), CommitVersion(5), value("five"));
		tier.insert(key.clone(), CommitVersion(9), value("nine"));

		assert!(matches!(tier.get(&key, CommitVersion(2)), VersionedGetResult::NotFound));
		assert_eq!(
			totals(&tier),
			MultiReadMetrics {
				hits: 0,
				previous_hits: 0,
				misses: 1
			},
			"the key was resident, so scoring residency alone would call this a hit and hide a reader the cache could not answer"
		);
	}

	#[test]
	fn a_cached_tombstone_reads_back_as_a_tombstone() {
		let tier = tier();
		let key = row_key(4);
		tier.insert(key.clone(), CommitVersion(5), None);

		assert!(matches!(tier.get(&key, CommitVersion(7)), VersionedGetResult::Tombstone));
		assert_eq!(
			totals(&tier),
			MultiReadMetrics {
				hits: 1,
				previous_hits: 0,
				misses: 0
			}
		);
	}

	#[test]
	fn an_invalidated_key_reads_as_a_miss() {
		let tier = tier();
		let key = row_key(5);
		tier.insert(key.clone(), CommitVersion(5), value("five"));
		tier.invalidate(&key);

		assert!(matches!(tier.get(&key, CommitVersion(7)), VersionedGetResult::NotFound));
		assert_eq!(
			totals(&tier),
			MultiReadMetrics {
				hits: 0,
				previous_hits: 0,
				misses: 1
			}
		);
	}

	#[test]
	fn a_clone_shares_the_entries_and_the_counters() {
		// Two handles must be one cache. A clone that copied the shard array instead of sharing it would
		// serve stale rows and tally its own reads, and neither shows up as a compile error.
		let original = tier();
		let clone = original.clone();
		original.insert(row_key(9), CommitVersion(5), value("five"));

		assert!(
			matches!(clone.get(&row_key(9), CommitVersion(7)), VersionedGetResult::Value { .. }),
			"a clone must observe a write made through the original"
		);
		assert_eq!(totals(&original).hits, 1, "the read counters must be shared, not duplicated per handle");
	}

	#[test]
	fn accounting_survives_supersede_echo_and_invalidate_churn() {
		// Every mutation path charges and releases; a single mis-charge leaves the total drifted. Comparing
		// against a tier holding only the survivor pins the exact figure without restating the per-entry
		// overhead, which would rot the moment the entry layout changes.
		let churned = tier();
		churned.insert(row_key(1), CommitVersion(5), value("aaa"));
		churned.insert(row_key(1), CommitVersion(9), value("bbbbb"));
		churned.insert(row_key(1), CommitVersion(9), value("bbbbb"));
		churned.insert(row_key(2), CommitVersion(5), value("cc"));
		churned.insert(row_key(2), CommitVersion(9), value("d"));
		churned.invalidate(&row_key(2));
		churned.insert(row_key(3), CommitVersion(5), value("x"));
		churned.invalidate(&row_key(3));

		let survivor = tier();
		survivor.insert(row_key(1), CommitVersion(9), value("bbbbb"));

		assert_eq!(
			used(&churned),
			used(&survivor),
			"after a supersede, an echo that clears the displaced slot, and two invalidates, only one entry remains and the total must say so"
		);
		assert!(used(&churned) > 0, "an empty total would satisfy the comparison without proving anything");
	}
}
