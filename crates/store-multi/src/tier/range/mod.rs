// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	default,
	interface::{
		catalog::storage::StorageId,
		store::{EntryKind, classify_key},
	},
	key::{
		row::RowKey,
		typed::{ExclusiveUpperEnd, Key, MultiKey, range::KeyRange},
	},
};
use reifydb_store::{
	coverage::{
		cursor::{RangeCursor as TierCursor, ServedChunk as TierChunk},
		interval::Interval,
		plan::{DEFAULT_GAP_GUARD, Segment},
	},
	tier::range::{
		Materialize, RangeConfig, RangeDomain, RangeMetrics, RangeRows, RangeShardMetrics, RangeTier, RowBytes,
	},
};
use reifydb_store_commit::{MultiVersionScope, RangeBatch, RangeCursor, RawEntry};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};

#[derive(Clone, Copy, Debug)]
pub struct MultiRangeConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
	pub gap_guard: usize,
}

impl MultiRangeConfig {
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(default::store::MULTI_RANGE_BUFFER_SHARD_TESTING),
			shards: default::store::MULTI_RANGE_BUFFER_SHARDS_TESTING as usize,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}
}

impl From<MultiRangeConfig> for RangeConfig {
	fn from(config: MultiRangeConfig) -> Self {
		Self {
			shard_bytes: config.shard_bytes,
			shards: config.shards,
			gap_guard: config.gap_guard,
		}
	}
}
pub type ServedChunk = reifydb_store::coverage::cursor::ServedChunk<RangeBatch>;

const ROW_BUCKET_SHIFT: u32 = 16;
const KIND_BYTES: usize = 1;
const STORAGE_ID_BYTES: usize = 9;
const BAND_BYTES: usize = KIND_BYTES + STORAGE_ID_BYTES;
const BUCKET_BYTES: usize = (u64::BITS - ROW_BUCKET_SHIFT) as usize / 8;

#[derive(Clone, Copy, Debug)]
pub struct MultiDomain;

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PartitionId {
	pub kind: EntryKind,
	pub bucket: u64,
}

impl PartitionId {
	pub const PREFIX_LEN: usize = BAND_BYTES + BUCKET_BYTES;

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

	pub fn prefix(&self) -> EncodedKey {
		let mut bytes = RowKey::storage_start(self.storage()).as_slice().to_vec();
		bytes.extend_from_slice(&self.bucket.to_be_bytes()[8 - BUCKET_BYTES..]);
		EncodedKey::new(bytes)
	}

	pub fn span(&self) -> (MultiKey, ExclusiveUpperEnd<MultiKey>) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => ExclusiveUpperEnd::of(successor),
			None => ExclusiveUpperEnd::Top,
		};
		(start, end)
	}

	fn first_addressable(key: &EncodedKey) -> Option<EncodedKey> {
		let bytes = key.as_slice();
		if bytes.len() >= Self::PREFIX_LEN {
			return None;
		}
		let mut padded = bytes.to_vec();
		padded.resize(Self::PREFIX_LEN, 0);
		Some(EncodedKey::new(padded))
	}
}

pub fn row_band(kind: EntryKind) -> Option<(EncodedKey, EncodedKey)> {
	match kind {
		EntryKind::Source(storage) => Some((RowKey::storage_start(storage), RowKey::storage_end(storage))),
		_ => None,
	}
}

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
	type Key = MultiKey;
	type MetricBucket = ();
	type Row = MultiRow;

	const METRIC_BUCKETS: usize = 1;

	const SCOPE: &'static str = "multi_range";

	const GAP_SCOPE: &'static str = "multi_range::gaps";

	fn partition(dimension: Self::Dimension, key: &Self::Key) -> Option<Self::Partition> {
		PartitionId::of(dimension, key)
	}

	fn first_addressable(key: &Self::Key) -> Option<Self::Key> {
		PartitionId::first_addressable(key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.kind
	}

	fn span(partition: &Self::Partition) -> (Self::Key, ExclusiveUpperEnd<Self::Key>) {
		partition.span()
	}

	fn head_band(dimension: Self::Dimension) -> Option<(Self::Key, Self::Key)> {
		row_band(dimension)
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		partition.kind.cache_tiers().caches_ranges()
	}

	fn cache_tiers_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd<Self::Key> {
		ExclusiveUpperEnd::Key(RowKey::storage_end(partition.storage()))
	}

	fn supersedes(resident: &Self::Row, incoming: &Self::Row) -> bool {
		incoming.version >= resident.version
	}

	fn admits_unproven_writes() -> bool {
		true
	}

	fn metric_bucket(_partition: &Self::Partition) -> usize {
		0
	}

	fn metric_bucket_at(_index: usize) -> Self::MetricBucket {}

	fn metric_bucket_name(_slot: Self::MetricBucket) -> Cow<'static, str> {
		Cow::Borrowed("row")
	}
}

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
		let tier = RangeTier::new(config.into())?;
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
		if !table.cache_tiers().caches_ranges() {
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
		let Some(scan) = self.tier.plan_scan(table, &KeyRange::from(&range)) else {
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
		let span = Interval::new(lo.clone(), ExclusiveUpperEnd::just_past(through));
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
		if descending || !table.cache_tiers().caches_ranges() {
			return ServedChunk::Gap;
		}
		let range_lo = EncodedKey::new(start);
		let range_hi = EncodedKey::new(end);
		if range_lo > range_hi {
			return ServedChunk::Gap;
		}
		let lo = match cursor.last_key() {
			Some(last) if *last >= range_lo => last.successor(),
			_ => Some(range_lo.clone()),
		};
		let Some(lo) = lo.filter(|lo| *lo <= range_hi) else {
			return ServedChunk::Gap;
		};
		let hi = ExclusiveUpperEnd::just_past(&range_hi);
		let range = EncodedKeyRange::new(Bound::Included(lo.clone()), Bound::Included(range_hi.clone()));

		let Some(scan) = self.tier.plan_scan(table, &KeyRange::from(&range)) else {
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

fn band_ends_the_range(table: EntryKind, stop: &ExclusiveUpperEnd<MultiKey>, range_hi: &EncodedKey) -> bool {
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
		key::{EncodableKey, row::RowKey, series_row::SeriesRowKey, typed::range::KeyRange},
	};
	use reifydb_store::coverage::plan::DEFAULT_GAP_GUARD;
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::row_number::RowNumber};

	use super::{
		Bound, EncodedKey, EncodedKeyRange, EntryKind, ExclusiveUpperEnd, MultiDomain, MultiRangeConfig,
		MultiRangeTier, MultiVersionScope, PartitionId, ROW_BUCKET_SHIFT, RangeCursor, RangeDomain, RawEntry,
		Segment, ServedChunk,
	};

	const STORAGE: StorageId = StorageId::Table(TableId(1));
	const NEIGHBOUR: StorageId = StorageId::Table(TableId(0));

	fn tier() -> MultiRangeTier {
		MultiRangeTier::new(MultiRangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 4,
			gap_guard: DEFAULT_GAP_GUARD,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	const BUCKET: u64 = 1 << ROW_BUCKET_SHIFT;

	fn tight() -> MultiRangeTier {
		MultiRangeTier::new(MultiRangeConfig {
			shard_bytes: Some(ByteSize::from_kib(4)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn row(n: u64) -> EncodedKey {
		RowKey {
			storage: STORAGE,
			row: RowNumber(n),
		}
		.encode()
	}

	fn series(n: u64) -> EncodedKey {
		SeriesRowKey {
			storage: STORAGE,
			variant_tag: None,
			key: n,
			sequence: 0,
		}
		.encode()
	}

	fn source() -> EntryKind {
		EntryKind::Source(STORAGE)
	}

	fn entry(n: u64, version: u64) -> RawEntry {
		RawEntry {
			key: row(n),
			version: CommitVersion(version),
			value: Some(CowVec::new(version.to_be_bytes().to_vec())),
		}
	}

	fn newest() -> MultiVersionScope {
		MultiVersionScope::AsOf {
			read: CommitVersion(u64::MAX),
		}
	}

	fn storage_start() -> EncodedKey {
		RowKey::storage_start(STORAGE)
	}

	fn storage_end() -> EncodedKey {
		RowKey::storage_end(STORAGE)
	}

	/// Materializes a chunk of a scan that began at the storage prefix and ran to the storage end, which is
	/// the shape of every full scan in this codebase; `rows` must be listed in encoded key order, so
	/// descending by row number.
	fn materialize_from_prefix(tier: &MultiRangeTier, rows: &[u64], version: u64) {
		let entries: Vec<RawEntry> = rows.iter().map(|n| entry(*n, version)).collect();
		tier.materialize_scanned_chunk(source(), &storage_start(), &storage_end(), &entries);
	}

	fn serve_whole_storage(tier: &MultiRangeTier, cursor: &mut RangeCursor) -> ServedChunk {
		tier.serve_persistent_chunk(
			source(),
			cursor,
			storage_start().as_slice(),
			storage_end().as_slice(),
			newest(),
			64,
			false,
		)
	}

	fn head_advances(tier: &MultiRangeTier) -> u64 {
		tier.serve_metrics().iter().map(|shard| shard.head_advances).sum()
	}

	/// Row keys invert the row number, so the highest row in a bucket is its lowest key: a forward scan
	/// over rows 0..n runs from `row(n)` down to `row(0)`.
	fn serve(
		tier: &MultiRangeTier,
		cursor: &mut RangeCursor,
		lo_row: u64,
		hi_row: u64,
		batch: usize,
	) -> ServedChunk {
		let start = row(hi_row);
		let end = row(lo_row);
		tier.serve_persistent_chunk(source(), cursor, start.as_slice(), end.as_slice(), newest(), batch, false)
	}

	fn rows_of(chunk: &ServedChunk) -> Vec<u64> {
		match chunk {
			ServedChunk::Served(batch) => batch
				.entries
				.iter()
				.map(|e| RowKey::decode(&e.key).expect("a served row key must decode").row.0)
				.collect(),
			ServedChunk::Gap => panic!("expected a served chunk, got a gap"),
		}
	}

	fn is_gap(chunk: &ServedChunk) -> bool {
		matches!(chunk, ServedChunk::Gap)
	}

	fn fill_bucket(tier: &MultiRangeTier, bucket: u64, rows: &[u64], version: u64) {
		// A scan yields entries in ascending key order, which row keys invert into descending row number, so
		// the caller's order cannot be trusted.
		let base = bucket * BUCKET;
		let mut entries: Vec<RawEntry> = rows.iter().map(|n| entry(*n, version)).collect();
		entries.sort_by(|left, right| left.key.cmp(&right.key));
		assert!(
			tier.materialize_scanned_chunk(source(), &row(base + BUCKET - 1), &row(base), &entries),
			"a whole-bucket chunk must publish its claim"
		);
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
		// The persistent read feeding a claim can predate a flushed row, so a write the cache declined is lost
		// under it.
		let tier = tier();
		let kind = EntryKind::Source(STORAGE);
		let flushed = RowKey::encoded(STORAGE, 5);
		let lo = RowKey::encoded(STORAGE, 9);
		let through = RowKey::encoded(STORAGE, 1);
		assert!(
			lo < through,
			"row keys encode descending, so the low end of the span is the highest row number"
		);

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
		let entry =
			batch.entries.iter().find(|entry| entry.key == key).expect("the row must still be resident");
		assert_eq!(entry.version, CommitVersion(5), "the older write must not have displaced the newer row");
		assert_eq!(entry.value.as_ref().expect("a value, not a tombstone").as_ref(), b"v5");
	}

	#[test]
	fn evicting_the_partition_the_head_came_from_leaves_the_head_standing() {
		// Eviction takes rows out of ram; it cannot put one into the persistent tier. The head asserts only
		// that the persistent tier is empty below it, so it must outlive every row that produced it. That is
		// why it is kept apart from the claims: a claim must die with its partition, while a proof of absence
		// that died with its partition is lost on the first turnover and every scan falls through at the
		// storage prefix again.
		let tier = tight();
		let entries = vec![entry(BUCKET * 4 + 3, 1), entry(BUCKET * 4 + 2, 1), entry(BUCKET * 4 + 1, 1)];
		assert!(
			tier.materialize_scanned_chunk(source(), &storage_start(), &storage_end(), &entries),
			"the chunk must publish its claim, or the test never reaches the case it is here to pin"
		);
		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(BUCKET * 4 + 3)),
			"the materialize must have recorded a head"
		);
		assert!(
			tier.tier.lookup(source(), &row(BUCKET * 4 + 2)).is_some(),
			"the materialize must have published a claim"
		);

		for n in 1..=512 {
			tier.insert(row(n), CommitVersion(1), Some(CowVec::new(vec![n as u8; 8])));
		}

		assert!(
			tier.tier.lookup(source(), &row(BUCKET * 4 + 2)).is_none(),
			"the evicted partition's claim must be withdrawn, or it answers for rows ram no longer holds"
		);
		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(BUCKET * 4 + 3)),
			"eviction cannot create a row, so the proof of absence must survive it"
		);
	}

	#[test]
	fn a_row_placed_into_ram_below_the_head_pulls_the_head_back_to_it() {
		// A flush writes a row to the persistent tier and only then seeds it here, so from this call on the
		// persistent tier may hold it. A head left above it makes every later scan begin past the row and
		// never read it from any tier. Placing a row can only ever be evidence that the span below the head
		// is not empty after all, so the head must yield to it.
		let tier = tier();
		tier.tier.raise_head(
			source(),
			&storage_start(),
			&storage_end(),
			Some(&row(3)),
			tier.tier.retractions(),
		);

		tier.insert(row(7), CommitVersion(1), Some(CowVec::new(vec![1])));

		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(7)),
			"a row placed inside the head span must pull the head back to it"
		);
	}

	#[test]
	fn a_head_raise_that_read_its_token_before_a_withdrawal_publishes_nothing() {
		// The scan that proves a span empty runs under no lock, so a commit can place a row inside that span
		// between the scan and the raise. Publishing the raise anyway makes every later scan start past the
		// new row and never read it from any tier, with no gap and no error to show for it.
		let tier = tier();
		let token = tier.tier.retractions();

		tier.invalidate(&row(7));

		tier.tier.raise_head(source(), &storage_start(), &storage_end(), Some(&row(3)), token);
		assert_eq!(tier.tier.head(source()), None, "a head published across a withdrawal");

		tier.tier.raise_head(
			source(),
			&storage_start(),
			&storage_end(),
			Some(&row(3)),
			tier.tier.retractions(),
		);
		assert_eq!(tier.tier.head(source()).as_ref(), Some(&row(3)), "a fresh token must publish");
	}

	#[test]
	fn a_scan_below_the_row_band_never_raises_a_head_over_it() {
		// Row keys and series row keys of one storage share an entry kind but occupy disjoint byte bands,
		// with the series band wholly below the row band. A series scan proves nothing about the rows, so a
		// head raised from one would report every row of the storage absent.
		let tier = tier();

		tier.tier.raise_head(source(), &series(9), &storage_end(), Some(&series(1)), tier.tier.retractions());

		assert_eq!(
			tier.tier.head(source()),
			None,
			"a scan that never entered the row band proved nothing about it"
		);
	}

	#[test]
	fn a_scan_starting_at_a_storage_prefix_serves_once_the_head_names_the_first_row() {
		// A scan starts at a ten byte storage prefix that sorts below every key of the storage, so no claim
		// can ever reach it and the leading chunk of every scan falls through. Where scans are one chunk long
		// that is every chunk, and the tier answers nothing at all. One recorded key proving the span below
		// the first row empty is enough to move the scan onto a partition a claim does cover, and it is the
		// only thing that can be: where the first row lies cannot be derived, only observed.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);

		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&tier, &mut cursor);

		assert_eq!(rows_of(&chunk), vec![3, 2, 1], "the leading chunk of a prefix scan must serve from ram");
		assert!(cursor.is_exhausted(), "the claim reaches the storage end, so nothing is left for persistent");
		assert_eq!(
			head_advances(&tier),
			1,
			"the serve must be attributed to the head, not to a claim over the prefix"
		);
	}

	#[test]
	fn a_commit_below_the_head_pulls_it_back_and_stops_the_scan_skipping_the_new_row() {
		// The head proves the persistent tier holds nothing below it. A commit places a row inside that span,
		// so a head left standing makes every later scan begin past the new row. That loss is silent: the
		// chunk is served, not gapped, and reports the range exhausted, so the row is never read from any
		// tier.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);
		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(3)),
			"the materialize must have recorded a head"
		);

		tier.invalidate(&row(7));

		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(7)),
			"a row committed inside the head span must pull the head back to it"
		);
		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&tier, &mut cursor);
		assert!(
			is_gap(&chunk),
			"the span the commit landed in is no longer claimed, so the scan must fall through"
		);
		assert!(!cursor.is_exhausted(), "a gap must leave the cursor untouched");
	}

	#[test]
	fn the_head_never_moves_a_scan_past_the_end_of_its_own_range() {
		// The head names the first row of the whole storage, which can sort past the end of a narrower range.
		// Moving lo there abandons the span the caller asked about and consults a claim over a span it did
		// not, so a range ram can prove empty falls through to the persistent tier instead.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);
		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(3)),
			"the materialize must have recorded a head"
		);

		let mut cursor = RangeCursor::new();
		let chunk = serve(&tier, &mut cursor, 5, 9, 64);

		assert!(rows_of(&chunk).is_empty(), "no row of this storage lies in rows five through nine");
		assert!(cursor.is_exhausted(), "the claim spans the whole range, so ram has proven it empty");
		assert_eq!(head_advances(&tier), 0, "the head sorts past this range and must not have been used");
	}

	#[test]
	fn a_range_below_the_row_band_is_never_moved_onto_it_by_the_head() {
		// One entry kind covers both a storage's row keys and its series row keys, and the two bands are
		// disjoint: they differ in their leading kind byte and the series band sorts wholly below the row
		// band. A head names a row key, so applying it to a range starting below that band moves the scan off
		// the keys the caller asked for and onto the rows, reporting everything below proven absent.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);
		tier.insert(series(1), CommitVersion(10), Some(CowVec::new(vec![1])));
		assert!(
			series(1).as_slice() < storage_start().as_slice(),
			"the series band must sort below the row band, or this range never crosses the boundary"
		);

		let mut cursor = RangeCursor::new();
		let chunk = tier.serve_persistent_chunk(
			source(),
			&mut cursor,
			series(9).as_slice(),
			storage_end().as_slice(),
			newest(),
			64,
			false,
		);

		assert!(is_gap(&chunk), "a range starting below the row band must never be answered from a row head");
		assert!(!cursor.is_exhausted(), "a gap must leave the cursor untouched");
		assert_eq!(head_advances(&tier), 0, "the head must not have been applied outside its own band");
	}

	#[test]
	fn an_empty_storage_is_read_from_persistent_once_and_never_again() {
		// Neither storage sentinel resolves to a row partition, so the head is the only proof an empty
		// storage can ever produce; without cashing it in every scan falls through to persistent forever.
		let tier = tier();

		let mut first = RangeCursor::new();
		assert!(is_gap(&serve_whole_storage(&tier, &mut first)), "nothing is proven before the first scan");

		tier.materialize_scanned_chunk(source(), &storage_start(), &storage_end(), &[]);

		let mut second = RangeCursor::new();
		let chunk = serve_whole_storage(&tier, &mut second);
		assert!(!is_gap(&chunk), "the proven-empty storage must never reach the persistent tier again");
		assert!(rows_of(&chunk).is_empty(), "a proven-empty range must serve no rows");
		assert!(
			second.is_exhausted(),
			"an empty range that is not exhausted hands the scan straight back to persistent"
		);
	}

	#[test]
	fn a_range_ending_on_the_head_is_never_answered_empty() {
		// The head names a key a row may sit on, so only the storage end sentinel, which no row can occupy,
		// may be answered as proven empty; answering at the head itself drops the row standing on it.
		let tier = tier();
		materialize_from_prefix(&tier, &[5, 3], 10);
		assert_eq!(
			tier.tier.head(source()).as_ref(),
			Some(&row(5)),
			"the materialize must name the first row as the head"
		);

		tier.invalidate(&row(5));
		tier.invalidate(&row(3));

		let mut cursor = RangeCursor::new();
		let chunk = tier.serve_persistent_chunk(
			source(),
			&mut cursor,
			storage_start().as_slice(),
			row(5).as_slice(),
			newest(),
			64,
			false,
		);
		assert!(
			is_gap(&chunk),
			"a range whose last key is the head itself is not proven empty and the persistent tier still owes it"
		);
		assert!(!cursor.is_exhausted(), "a gap must leave the cursor untouched");
	}

	#[test]
	fn a_serve_reports_exhausted_only_when_the_claim_reaches_past_the_range_end() {
		// Reporting the persistent tier exhausted is the one thing a serve can say that loses rows. It is only
		// true when ram has proven there is nothing left in the range, which is when the claim runs past the
		// range's last key and not merely to the last row ram happens to hold.
		let intact = tier();
		fill_bucket(&intact, 0, &[2, 4, 6], 10);

		let mut whole = RangeCursor::new();
		let chunk = serve(&intact, &mut whole, 0, BUCKET - 1, 64);
		assert_eq!(rows_of(&chunk), vec![6, 4, 2]);
		assert!(whole.is_exhausted(), "a claim spanning the whole range has proven the rest of it empty");

		let punched = tier();
		fill_bucket(&punched, 0, &[2, 4, 6], 10);
		punched.invalidate(&row(1));

		let mut clipped = RangeCursor::new();
		let chunk = serve(&punched, &mut clipped, 0, BUCKET - 1, 64);
		assert_eq!(rows_of(&chunk), vec![6, 4, 2], "the rows below the punched key are the same");
		assert!(
			!clipped.is_exhausted(),
			"the claim now ends at the punched key, so the persistent tier still owes the rest"
		);
	}

	#[test]
	fn a_claim_that_scanned_to_the_storage_end_reports_exhausted_there() {
		// Every scan ends at a storage end sentinel no row key can occupy, so without a tail rule the last
		// chunk of every scan falls through and buys one persistent read to confirm the range is over.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);

		let mut cursor = RangeCursor::new();
		cursor.advance(row(3));
		let chunk = serve_whole_storage(&tier, &mut cursor);

		assert!(
			cursor.is_exhausted(),
			"a claim that scanned to the storage end has proven the rest of it empty"
		);
		assert_eq!(rows_of(&chunk), vec![2, 1]);
	}

	#[test]
	fn a_claim_punched_short_of_the_storage_end_is_not_exhausted() {
		// The tail rule needs one claim reaching the band end; a claim clipped by a punched key proves
		// nothing past it and reporting exhausted there silently drops every remaining row.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);
		tier.invalidate(&row(1));

		let mut cursor = RangeCursor::new();
		cursor.advance(row(3));
		let chunk = serve_whole_storage(&tier, &mut cursor);

		assert!(!cursor.is_exhausted(), "the claim stops at the punched key, which proves nothing past it");
		assert_eq!(rows_of(&chunk), vec![2]);
	}

	#[test]
	fn a_claim_stopping_on_its_last_row_rather_than_past_it_is_not_exhausted() {
		// A chunk that stopped on a row ends its claim at that key rather than past the band, so the tail rule
		// must compare against the band end or it reports every row above the one it stopped on absent.
		let tier = tier();
		let entries = vec![entry(3, 10), entry(2, 10), entry(1, 10)];
		assert!(
			tier.materialize_scanned_chunk(source(), &storage_start(), &row(1), &entries),
			"the chunk must publish its claim, or the test never reaches the case it is here to pin"
		);

		let mut cursor = RangeCursor::new();
		cursor.advance(row(3));
		let chunk = serve_whole_storage(&tier, &mut cursor);

		assert!(
			!cursor.is_exhausted(),
			"the claim stops on the last row it read, which proves nothing past it"
		);
		assert_eq!(rows_of(&chunk), vec![2, 1]);
	}

	#[test]
	fn a_claim_over_a_partition_that_is_not_the_last_is_not_exhausted_at_the_storage_end() {
		// Every scan ends at the storage end, so a tail rule keyed on the range rather than on the segment
		// reaching the band end would report exhausted on the first partition served to its edge and drop
		// every partition below it.
		let tier = tier();
		fill_bucket(&tier, 1, &[BUCKET + 1, BUCKET + 2], 10);
		fill_bucket(&tier, 0, &[1, 2], 10);

		let mut cursor = RangeCursor::new();
		cursor.advance(row(BUCKET + 2));
		let chunk = serve_whole_storage(&tier, &mut cursor);

		assert!(
			!cursor.is_exhausted(),
			"the lower partition is a separate claim the persistent tier still owes"
		);
		assert_eq!(rows_of(&chunk), vec![BUCKET + 1]);
	}

	#[test]
	fn a_range_reaching_past_the_storage_end_is_never_reported_exhausted() {
		// A range is classified by its start, so its end may lie in another storage whose rows this claim says
		// nothing about; reporting exhausted there drops all of them.
		let tier = tier();
		materialize_from_prefix(&tier, &[3, 2, 1], 10);

		let end = RowKey::encoded(NEIGHBOUR, 5);
		let mut cursor = RangeCursor::new();
		cursor.advance(row(3));
		let chunk = tier.serve_persistent_chunk(
			source(),
			&mut cursor,
			storage_start().as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(!cursor.is_exhausted(), "the claim says nothing about the storage the range runs on into");
		assert_eq!(rows_of(&chunk), vec![2, 1]);
		assert!(
			end.as_slice() > storage_end().as_slice(),
			"the range end must really sort past this storage, or the case under test never arose"
		);
	}

	#[test]
	fn a_claim_serves_a_partition_no_longer_covered_end_to_end() {
		// A commit anywhere in a partition withdraws only the one key that left ram; everything either side of
		// it must still serve from the claim, where a whole-partition claim would serve nothing at all.
		let tier = tier();
		fill_bucket(&tier, 0, &[1, 2, 3, 4, 5], 10);
		tier.invalidate(&row(3));

		let mut cursor = RangeCursor::new();
		let chunk = serve(&tier, &mut cursor, 0, BUCKET - 1, 64);

		assert_eq!(
			rows_of(&chunk),
			vec![5, 4],
			"the claim below the punched key must still serve, where a whole-partition claim serves nothing"
		);
		assert!(!cursor.is_exhausted(), "a claim that stops at the punched key has proven nothing beyond it");
	}

	#[test]
	fn a_scan_starting_at_a_storage_prefix_is_not_claimed_and_falls_through() {
		// Every range scan starts at a ten byte storage prefix, which no claim reaches because a claim's lower
		// end is always a key a materialize observed. The leading chunk of a scan is therefore the persistent
		// tier's, and a serve that answered it would be inventing a proof no scan ever made.
		let tier = tier();
		fill_bucket(&tier, 0, &[1, 2, 3], 10);

		let range = EncodedKeyRange::new(Bound::Included(storage_start()), Bound::Included(storage_end()));
		let plan = tier
			.tier
			.plan_scan(source(), &KeyRange::from(&range))
			.expect("a whole storage must be plannable");
		assert!(
			matches!(plan.segments().first(), Some(Segment::Gap { .. })),
			"a claim reached below the lowest key its materialize observed, down to a prefix nothing proved"
		);

		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&tier, &mut cursor);
		assert!(is_gap(&chunk), "no claim covers the prefix the scan starts at");

		cursor.advance(row(3));
		let resumed = serve_whole_storage(&tier, &mut cursor);
		assert_eq!(rows_of(&resumed), vec![2, 1], "once the cursor is on a real key the claim serves");
	}

	#[test]
	fn a_partition_span_never_reaches_the_series_band_of_the_same_storage() {
		// One entry kind covers a storage's row keys and its series row keys, and the series band sorts wholly
		// below the row band. A span reaching into it would retract coverage over keys the partition never
		// held, and a series key answered from a row partition reads as a row that is not there.
		let bucket = PartitionId::of(source(), &row(1)).expect("a row key must name a partition");
		let (start, _) = bucket.span();

		assert!(series(1).as_slice() < start.as_slice(), "the series band must sort below every row partition");
		assert_eq!(PartitionId::of(source(), &series(1)), None, "a series key must name no row partition");
		assert_eq!(
			PartitionId::of(source(), &series(u64::MAX)),
			None,
			"no series key of the band may be attributed to a row partition"
		);
	}

	#[test]
	fn a_partition_span_never_reaches_the_top_of_the_key_space() {
		// A span running to the top of the key space retracts the coverage of everything sorting above it, so
		// the row band must always leave a successor for the span to stop at.
		for storage in [STORAGE, NEIGHBOUR, StorageId::Table(TableId(u64::MAX))] {
			for bucket in [0u64, 1, u64::MAX >> ROW_BUCKET_SHIFT] {
				let (_, end) = PartitionId {
					kind: EntryKind::Source(storage),
					bucket,
				}
				.span();
				assert!(
					!matches!(end, ExclusiveUpperEnd::Top),
					"partition {bucket} of {storage:?} spans to the top of the key space"
				);
			}
		}
	}
}
