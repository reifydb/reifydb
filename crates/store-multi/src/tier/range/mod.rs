// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Range tier of the multi-version store: the shared partial-coverage cache, instantiated over multi's
//! keys and rows.
//!
//! A dimension is one entry kind, so every claim is scoped to the storage it was proven for. A partition
//! is the run of row keys sharing one `(kind, bucket)` prefix, where the bucket is the leading bytes of
//! the encoded row number; a key that names no such prefix is declined rather than cached, which is what
//! keeps the series band, whose keys sit below the row band under the same entry kind, out of a partition
//! whose span could never contain it.
//!
//! The version scope filter lives here rather than in the tier: the tier stores whatever row the domain
//! names and knows nothing about which versions a reader may see, so a served chunk is filtered before it
//! reaches the store and an emptied chunk that has not exhausted its range reads as a gap.

use std::{
	borrow::Cow,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::storage::StorageId,
		store::{EntryKind, classify_key},
	},
	key::row::RowKey,
};
use reifydb_store::{
	coverage::{
		ExclusiveUpperEnd,
		cursor::{RangeCursor as TierCursor, ServedChunk as TierChunk},
		interval::Interval,
		plan::Segment,
		successor,
	},
	tier::range::{
		Materialize, RangeConfig, RangeDomain, RangeMetrics, RangeRows, RangeShardMetrics, RangeTier,
		RowBytes, prefix_successor,
	},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};

use crate::{
	MultiVersionScope,
	tier::{RangeBatch, RangeCursor, RawEntry},
};

pub type MultiRangeConfig = RangeConfig;
pub type ServedChunk = reifydb_store::coverage::cursor::ServedChunk<RangeBatch>;

const ROW_BUCKET_SHIFT: u32 = 16;
const KIND_BYTES: usize = 1;
const STORAGE_ID_BYTES: usize = 9;
const BAND_BYTES: usize = KIND_BYTES + STORAGE_ID_BYTES;
const BUCKET_BYTES: usize = (u64::BITS - ROW_BUCKET_SHIFT) as usize / 8;

/// The multi store's range domain: a dimension is one entry kind and a partition is one bucket of the
/// row band that kind owns.
#[derive(Clone, Copy, Debug)]
pub struct MultiDomain;

/// One bucket of row keys: the unit of row storage, sharding, budgeting and eviction.
///
/// Coverage is not partitioned. Two claims over adjacent buckets coalesce into one interval, which is why
/// eviction retracts a bucket's whole span rather than a named interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PartitionId {
	pub kind: EntryKind,
	pub bucket: u64,
}

impl PartitionId {
	pub const PREFIX_LEN: usize = BAND_BYTES + BUCKET_BYTES;

	/// Reads the bucket prefix straight off the encoded key, since decoding it would fail on the short
	/// key a partition mapping is handed and read every key back as uncacheable.
	pub fn of(dimension: EntryKind, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		if bytes.len() < Self::PREFIX_LEN {
			return None;
		}
		let (band, _) = row_band(dimension)?;
		reifydb_assertions! {
			assert_eq!(
				band.len(),
				BAND_BYTES,
				"the row band prefix must be the kind byte plus the storage id, or the bucket bytes are read from the wrong offset and every claim covers a span it never proved"
			);
		}
		if bytes[..BAND_BYTES] != *band.as_slice() {
			return None;
		}
		let mut bucket = [0u8; 8];
		bucket[8 - BUCKET_BYTES..].copy_from_slice(&bytes[BAND_BYTES..Self::PREFIX_LEN]);
		Some(Self {
			kind: dimension,
			bucket: u64::from_be_bytes(bucket),
		})
	}

	fn storage(&self) -> StorageId {
		match self.kind {
			EntryKind::Source(storage) => storage,
			_ => panic!("a range partition outside a source entry kind names no row band"),
		}
	}

	/// The encoded `kind || storage || bucket` prefix, which must round-trip [`PartitionId::of`].
	pub fn prefix(&self) -> EncodedKey {
		let mut bytes = RowKey::storage_start(self.storage()).as_slice().to_vec();
		bytes.extend_from_slice(&self.bucket.to_be_bytes()[8 - BUCKET_BYTES..]);
		EncodedKey::new(bytes)
	}

	/// The half-open span this partition owns, so a whole partition retracts in one shrink.
	pub fn span(&self) -> (EncodedKey, ExclusiveUpperEnd) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => ExclusiveUpperEnd::of(successor),
			None => ExclusiveUpperEnd::Top,
		};
		(start, end)
	}
}

/// The inclusive band of row keys one entry kind owns, and the only span a head may prove empty.
pub fn row_band(kind: EntryKind) -> Option<(EncodedKey, EncodedKey)> {
	match kind {
		EntryKind::Source(storage) => Some((RowKey::storage_start(storage), RowKey::storage_end(storage))),
		_ => None,
	}
}

/// One cached version of a key, which must carry the version or a scoped reader filters on nothing.
#[derive(Clone, Debug)]
pub struct MultiRow {
	pub version: CommitVersion,
	pub value: Option<CowVec<u8>>,
}

impl RowBytes for MultiRow {
	fn row_bytes(&self) -> usize {
		self.value.as_ref().map_or(0, |value| value.len())
	}
}

impl RangeDomain for MultiDomain {
	type Dimension = EntryKind;
	type Partition = PartitionId;
	type Slot = ();
	type Row = MultiRow;

	const PREFIX_LEN: usize = PartitionId::PREFIX_LEN;
	const SLOTS: usize = 1;

	const SCOPE: &'static str = "multi_range";

	const GAP_SCOPE: &'static str = "multi_range::gaps";

	fn partition(dimension: Self::Dimension, key: &EncodedKey) -> Option<Self::Partition> {
		PartitionId::of(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.kind
	}

	fn span(partition: &Self::Partition) -> (EncodedKey, ExclusiveUpperEnd) {
		partition.span()
	}

	fn head_band(dimension: Self::Dimension) -> Option<(EncodedKey, EncodedKey)> {
		row_band(dimension)
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		partition.kind.cache_policy().caches_ranges()
	}

	fn policy_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd {
		ExclusiveUpperEnd::Key(RowKey::storage_end(partition.storage()))
	}

	fn supersedes(resident: &Self::Row, incoming: &Self::Row) -> bool {
		incoming.version >= resident.version
	}

	fn admits_unproven_writes() -> bool {
		true
	}

	fn slot(_partition: &Self::Partition) -> usize {
		0
	}

	fn slot_at(_index: usize) -> Self::Slot {}

	fn slot_name(_slot: Self::Slot) -> Cow<'static, str> {
		Cow::Borrowed("row")
	}
}

/// Everything one shard's range tier reports, joined here because the three sources are indexed by shard
/// and only line up while they are the same length.
#[derive(Clone, Copy, Debug)]
pub struct MultiRangeShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub partitions: usize,
	pub entries: usize,
	pub complete_partitions: usize,
	pub counters: RangeMetrics,
	pub serve: MultiServeMetrics,
}

/// What one shard's range serves carried, counted here because the shared tier serves per segment while
/// the store reads per chunk and only the chunk is comparable to the page tier's numbers.
#[derive(Clone, Copy, Debug, Default)]
pub struct MultiServeMetrics {
	pub served: u64,
	pub rows: u64,
	pub head_advances: u64,
}

#[derive(Default)]
struct ServeCounters {
	served: AtomicU64,
	rows: AtomicU64,
	head_advances: AtomicU64,
}

#[derive(Clone)]
pub struct MultiRangeTier {
	tier: RangeTier<MultiDomain>,
	serves: Arc<[ServeCounters]>,
}

impl MultiRangeTier {
	pub fn new(config: MultiRangeConfig) -> Option<Self> {
		let tier = RangeTier::new(config)?;
		let shards = config.shards.max(1);
		Some(Self {
			tier,
			serves: (0..shards).map(|_| ServeCounters::default()).collect(),
		})
	}

	pub fn serve_metrics(&self) -> Vec<MultiServeMetrics> {
		self.serves
			.iter()
			.map(|counters| MultiServeMetrics {
				served: counters.served.load(Ordering::Relaxed),
				rows: counters.rows.load(Ordering::Relaxed),
				head_advances: counters.head_advances.load(Ordering::Relaxed),
			})
			.collect()
	}

	pub fn complete_partitions(&self) -> Vec<usize> {
		self.tier.complete_partitions()
	}

	pub fn insert(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		self.tier.insert(
			classify_key(&key),
			key,
			MultiRow {
				version,
				value,
			},
		);
	}

	pub fn invalidate(&self, key: &EncodedKey) {
		self.tier.invalidate(classify_key(key), key);
	}

	pub fn clear(&self) {
		self.tier.clear();
	}

	pub fn shard_metrics(&self) -> Vec<RangeShardMetrics> {
		self.tier.shard_metrics()
	}

	pub fn full_shard_metrics(&self) -> Vec<MultiRangeShardMetrics> {
		let shards = self.tier.shard_metrics();
		let serves = self.serve_metrics();
		let complete = self.complete_partitions();
		reifydb_assertions! {
			assert_eq!(
				(shards.len(), shards.len()),
				(serves.len(), complete.len()),
				"every shard must report all three sources, or a shard past the shortest reports zero forever"
			);
		}
		shards.into_iter()
			.zip(serves)
			.zip(complete)
			.map(|((shard, serve), complete_partitions)| MultiRangeShardMetrics {
				shard: shard.shard,
				used: shard.used,
				limit: shard.limit,
				partitions: shard.partitions,
				entries: shard.entries,
				complete_partitions,
				counters: shard.counters,
				serve,
			})
			.collect()
	}

	pub fn materialize_scanned_chunk(
		&self,
		table: EntryKind,
		lo: &EncodedKey,
		through: &EncodedKey,
		entries: &[RawEntry],
	) -> bool {
		if !table.cache_policy().caches_ranges() {
			return false;
		}
		self.tier.raise_head(
			table,
			lo,
			through,
			entries.first().map(|entry| &entry.key),
			self.tier.retractions(),
		);
		let range = EncodedKeyRange::new(Bound::Included(lo.clone()), Bound::Included(through.clone()));
		let Some(scan) = self.tier.plan_scan(table, &range) else {
			return false;
		};
		let rows: RangeRows<MultiDomain> = entries
			.iter()
			.map(|entry| {
				(
					entry.key.clone(),
					MultiRow {
						version: entry.version,
						value: entry.value.clone(),
					},
				)
			})
			.collect();
		let span = Interval::new(lo.clone(), ExclusiveUpperEnd::Key(successor(through)));
		matches!(self.tier.materialize(&scan, &span, &rows), Materialize::Materialized)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn serve_persistent_chunk(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		scope: MultiVersionScope,
		batch_size: usize,
		descending: bool,
	) -> ServedChunk {
		if descending || !table.cache_policy().caches_ranges() {
			return ServedChunk::Gap;
		}
		let range_lo = EncodedKey::new(start);
		let range_hi = EncodedKey::new(end);
		if range_lo > range_hi {
			return ServedChunk::Gap;
		}
		let lo = match cursor.last_key() {
			Some(last) if *last >= range_lo => successor(last),
			_ => range_lo.clone(),
		};
		if lo > range_hi {
			return ServedChunk::Gap;
		}
		let hi = ExclusiveUpperEnd::Key(successor(&range_hi));
		let range = EncodedKeyRange::new(Bound::Included(lo.clone()), Bound::Included(range_hi.clone()));

		let Some(scan) = self.tier.plan_scan(table, &range) else {
			return self.chunk_proven_empty(table, &lo, &range_hi, cursor);
		};
		let Some(Segment::Resident(segment)) = scan.segments().first() else {
			return self.chunk_proven_empty(table, &lo, &range_hi, cursor);
		};
		let Some(partition) = PartitionId::of(table, &segment.start) else {
			return ServedChunk::Gap;
		};
		let counters = &self.serves[self.tier.shard_index(&partition)];
		if scan.advanced() {
			counters.head_advances.fetch_add(1, Ordering::Relaxed);
		}

		let mut served = TierCursor::new();
		let TierChunk::Served(rows) = self.tier.serve(&scan, segment, &mut served, batch_size) else {
			return ServedChunk::Gap;
		};
		let out: Vec<RawEntry> = rows
			.into_iter()
			.filter(|(_, row)| scope.contains(row.version))
			.map(|(key, row)| RawEntry {
				key,
				version: row.version,
				value: row.value,
			})
			.collect();

		let exhausted = served.is_exhausted()
			&& (segment.end >= hi || band_ends_the_range(table, &segment.end, &range_hi));
		if !exhausted && out.is_empty() {
			return ServedChunk::Gap;
		}
		counters.served.fetch_add(1, Ordering::Relaxed);
		counters.rows.fetch_add(out.len() as u64, Ordering::Relaxed);
		served_chunk(out, cursor, exhausted)
	}

	fn chunk_proven_empty(
		&self,
		table: EntryKind,
		lo: &EncodedKey,
		range_hi: &EncodedKey,
		cursor: &mut RangeCursor,
	) -> ServedChunk {
		if self.tier.head_proves_empty(table, lo, range_hi) {
			return served_chunk(Vec::new(), cursor, true);
		}
		ServedChunk::Gap
	}
}

fn band_ends_the_range(table: EntryKind, stop: &ExclusiveUpperEnd, range_hi: &EncodedKey) -> bool {
	let Some((_, band_end)) = row_band(table) else {
		return false;
	};
	range_hi.as_slice() <= band_end.as_slice() && *stop >= ExclusiveUpperEnd::Key(band_end)
}

fn served_chunk(out: Vec<RawEntry>, cursor: &mut RangeCursor, exhausted: bool) -> ServedChunk {
	reifydb_assertions! {
		assert!(
			exhausted || !out.is_empty(),
			"a chunk that reports more must carry an entry, otherwise last_key never advances and the store's scan loop, which now ends only when every tier cursor is exhausted, spins forever"
		);
	}
	if let Some(last) = out.last() {
		cursor.advance(last.key.clone());
	}
	if exhausted {
		cursor.finish();
	}
	ServedChunk::Served(RangeBatch {
		entries: out,
		has_more: !exhausted,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{id::TableId, storage::StorageId},
		key::row::RowKey,
	};
	use reifydb_store::coverage::plan::DEFAULT_GAP_GUARD;
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

	use super::{
		EncodedKey, EntryKind, MultiDomain, MultiRangeConfig, MultiRangeTier, MultiVersionScope, RangeCursor,
		RangeDomain, RawEntry, ServedChunk,
	};

	const STORAGE: StorageId = StorageId::Table(TableId(1));

	fn tier() -> MultiRangeTier {
		MultiRangeTier::new(MultiRangeConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 4,
			gap_guard: DEFAULT_GAP_GUARD,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	#[test]
	fn a_write_into_a_partition_no_claim_reached_is_still_seated() {
		// A declined write leaves a later materialize free to claim the span and answer the row absent.
		let tier = tier();

		tier.insert(RowKey::encoded(STORAGE, 1), CommitVersion(1), Some(CowVec::new(b"v".to_vec())));

		let entries: usize = tier.shard_metrics().iter().map(|shard| shard.entries).sum();
		assert_eq!(entries, 1, "the write was dropped, so a claim taken across it answers the row absent");
	}

	#[test]
	fn a_claim_taken_across_a_declined_write_must_not_answer_that_row_absent() {
		// The persistent read feeding a claim can predate a flushed row, so a write the cache declined is lost under it.
		let tier = tier();
		let kind = EntryKind::Source(STORAGE);
		let flushed = RowKey::encoded(STORAGE, 5);
		let lo = RowKey::encoded(STORAGE, 9);
		let through = RowKey::encoded(STORAGE, 1);
		assert!(lo < through, "row keys encode descending, so the low end of the span is the highest row number");

		tier.insert(flushed.clone(), CommitVersion(1), Some(CowVec::new(b"flushed".to_vec())));

		let stale = [RawEntry {
			key: lo.clone(),
			version: CommitVersion(1),
			value: Some(CowVec::new(b"scanned".to_vec())),
		}];
		assert!(
			tier.materialize_scanned_chunk(kind, &lo, &through, &stale),
			"the chunk must claim its span, or the test never reaches the case it is here to pin"
		);

		let mut cursor = RangeCursor::new();
		let served = tier.serve_persistent_chunk(
			kind,
			&mut cursor,
			lo.as_slice(),
			through.as_slice(),
			MultiVersionScope::AsOf {
				read: CommitVersion(10),
			},
			32,
			false,
		);
		let ServedChunk::Served(batch) = served else {
			panic!("a claimed span must serve from ram, or the claim bought nothing");
		};
		assert!(
			batch.entries.iter().any(|entry| entry.key == flushed),
			"the claim outranked a flushed row the persistent read never saw, so the row reads as absent"
		);
	}

	#[test]
	fn the_multi_domain_hands_durability_to_ram_rather_than_declining_a_write() {
		// Flipping this back makes every uncovered write a candidate for silent loss under a later claim.
		assert!(
			MultiDomain::admits_unproven_writes(),
			"multi hands a flushed row to ram unconditionally, or the row is lost between the buffer and the claim"
		);
	}

	#[test]
	fn a_key_the_domain_cannot_attribute_names_no_partition() {
		// A key outside the row band must be declined outright. Attributed to a neighbouring partition it
		// would fall under that partition's span, which would then answer for rows it never held.
		let stray = EncodedKey::new(vec![0u8, 1, 2]);
		assert_eq!(
			MultiDomain::partition(EntryKind::Source(STORAGE), &stray),
			None,
			"a key shorter than the band prefix carries no bucket to attribute it by"
		);
		assert_eq!(
			MultiDomain::partition(EntryKind::Multi, &RowKey::encoded(STORAGE, 5)),
			None,
			"a row key under a kind with no row band must not be attributed either"
		);
	}

	#[test]
	fn an_older_write_must_not_displace_a_newer_resident_row() {
		// A flush can deliver a version the cache has already moved past. Seating it would roll the cached
		// row backwards and serve a value the store no longer holds.
		let tier = tier();
		let kind = EntryKind::Source(STORAGE);
		let key = RowKey::encoded(STORAGE, 5);
		let through = RowKey::encoded(STORAGE, 1);

		let newer = [RawEntry {
			key: key.clone(),
			version: CommitVersion(5),
			value: Some(CowVec::new(b"v5".to_vec())),
		}];
		assert!(
			tier.materialize_scanned_chunk(kind, &key, &through, &newer),
			"the chunk must claim its span, or the write below never lands on a resident row"
		);

		tier.insert(key.clone(), CommitVersion(2), Some(CowVec::new(b"v2".to_vec())));

		let mut cursor = RangeCursor::new();
		let served = tier.serve_persistent_chunk(
			kind,
			&mut cursor,
			key.as_slice(),
			through.as_slice(),
			MultiVersionScope::AsOf {
				read: CommitVersion(10),
			},
			32,
			false,
		);
		let ServedChunk::Served(batch) = served else {
			panic!("the claimed span must serve from ram");
		};
		let entry = batch.entries.iter().find(|entry| entry.key == key).expect("the row must still be resident");
		assert_eq!(entry.version, CommitVersion(5), "the older write must not have displaced the newer row");
		assert_eq!(entry.value.as_ref().expect("a value, not a tombstone").as_ref(), b"v5");
	}
}
