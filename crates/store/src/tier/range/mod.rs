// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod domain;
mod head;
mod point;
mod pool;
mod scan;
#[cfg(test)]
mod tests;
mod write;

use std::{borrow::Cow, collections::HashMap, fmt::Debug, hash::Hash, mem::size_of, ops::Bound, sync::Arc};

use reifydb_codec::{key::encoded::EncodedKeyRange, row::pod::EncodedPodRow};
use reifydb_core::{
	key::typed::{Edge, MultiKey, TypedKey},
	util::{budget::MemoryBudget, sorted::SortedVecMap},
};
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_value::byte_size::ByteSize;

use crate::coverage::{
	entry::{Entry, PinnedCount},
	index::CoverageIndex,
	interval::Interval,
	plan::{DEFAULT_GAP_GUARD, GapHistogram, Segment},
	retraction::Retractions,
};

pub trait RowBytes {
	fn row_bytes(&self) -> usize;
}

impl RowBytes for EncodedPodRow {
	fn row_bytes(&self) -> usize {
		self.len()
	}
}

pub trait RangeDomain: Copy + Debug + 'static {
	type Dimension: Copy + Eq + Hash + Send + Sync + 'static;
	type Partition: Copy + Eq + Hash + Send + Sync + 'static;
	type Key: TypedKey;
	type MetricBucket: Copy + Eq + Debug + Send + Sync + 'static;
	type Row: RowBytes + Clone + Send + Sync + 'static;

	const METRIC_BUCKETS: usize;

	const SCOPE: &'static str;

	const GAP_SCOPE: &'static str;

	fn partition(_dimension: Self::Dimension, _key: &Self::Key) -> Option<Self::Partition> {
		None
	}

	fn first_addressable(_key: &Self::Key) -> Option<Self::Key> {
		None
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension;

	fn span(partition: &Self::Partition) -> (Self::Key, Edge<Self::Key>);

	fn head_band(dimension: Self::Dimension) -> Option<(Self::Key, Self::Key)>;

	fn caches_ranges(partition: &Self::Partition) -> bool;

	fn cache_tiers_run_end(partition: &Self::Partition) -> Edge<Self::Key>;

	fn partition_walk_end(_partition: &Self::Partition) -> Edge<Self::Key> {
		Edge::Top
	}

	fn supersedes(_resident: &Self::Row, _incoming: &Self::Row) -> bool {
		true
	}

	fn admits_unproven_writes() -> bool {
		false
	}

	fn metric_bucket(partition: &Self::Partition) -> usize;

	fn metric_bucket_at(index: usize) -> Self::MetricBucket;

	fn metric_bucket_name(bucket: Self::MetricBucket) -> Cow<'static, str>;
}

#[derive(Clone, Copy, Debug)]
pub struct RangeConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
	pub gap_guard: usize,
}

impl RangeConfig {
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(ByteSize::from_mib(4)),
			shards: 16,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}
}

pub type RangeRows<D> = Vec<(<D as RangeDomain>::Key, <D as RangeDomain>::Row)>;

pub fn scan_range(gap: &Interval<MultiKey>) -> EncodedKeyRange {
	let end = match &gap.end {
		Edge::Bottom => Bound::Excluded(gap.start.clone()),
		Edge::Key(key) => Bound::Excluded(key.clone()),
		Edge::Top => Bound::Unbounded,
	};
	EncodedKeyRange::new(Bound::Included(gap.start.clone()), end)
}

pub fn proven_span<K: TypedKey>(gap: &Interval<K>, last_key: Option<&K>, exhausted: bool) -> Option<Interval<K>> {
	if exhausted {
		return Some(gap.clone());
	}
	let last = last_key?;
	Some(Interval::new(gap.start.clone(), Edge::just_past(last).min(gap.end.clone())))
}

struct Partition<K, R> {
	entries: SortedVecMap<K, Entry<R>>,
	pinned: PinnedCount,
	bytes: usize,
	tick: u64,
	created: u64,
	materializes: u64,
	written_at: u64,
	covered: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Progress {
	created: u64,
	materializes: u64,
	written_at: u64,
}

impl<K, R> Partition<K, R> {
	fn progress(&self) -> Progress {
		Progress {
			created: self.created,
			materializes: self.materializes,
			written_at: self.written_at,
		}
	}
}

struct Shard<D: RangeDomain> {
	partitions: HashMap<D::Partition, Partition<D::Key, D::Row>>,
	budget: MemoryBudget,
	next_tick: u64,
	writes: u64,
	gaps: GapHistogram,
	metrics: RangeMetrics,
	bucket_metrics: BucketCounters,
}

const fn entry_overhead<K, R>() -> usize {
	size_of::<(K, Entry<R>)>()
}

#[cfg(test)]
const ENTRY_OVERHEAD: usize = entry_overhead::<MultiKey, EncodedPodRow>();

const fn partition_overhead<D: RangeDomain>() -> usize {
	size_of::<D::Partition>() + size_of::<Partition<D::Key, D::Row>>()
}

fn entry_footprint<K: TypedKey, R: RowBytes>(key: &K, entry: &Entry<R>) -> usize {
	entry_overhead::<K, R>() + key.heap_size() + entry.value().map_or(0, RowBytes::row_bytes)
}

fn account(bytes: &mut usize, budget: &MemoryBudget, old: usize, new: usize) {
	if new >= old {
		let delta = new - old;
		*bytes += delta;
		budget.charge(ByteSize::from_bytes(delta as u64));
	} else {
		let delta = old - new;
		*bytes -= delta;
		budget.release(ByteSize::from_bytes(delta as u64));
	}
}

pub struct RangeScan<D: RangeDomain> {
	pub(super) dimension: D::Dimension,
	pub(super) confined: Option<D::Partition>,
	pub(super) advanced: bool,
	pub(super) segments: Vec<Segment<D::Key>>,
	pub(super) gaps: usize,
	pub(super) degraded: bool,
	pub(super) retractions: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Materialize {
	Materialized,
	NothingCacheable,
	Refused,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeMetrics {
	pub hits: u64,
	pub misses: u64,
	pub exempt: u64,
	pub materializes: u64,
	pub materializes_refused: u64,
	pub materializes_raced: u64,
	pub evictions: u64,
	pub point_hits: u64,
	pub point_misses: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RangeShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub partitions: usize,
	pub entries: usize,
	pub counters: RangeMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct RangeBucketMetrics<D: RangeDomain> {
	pub bucket: D::MetricBucket,
	pub used: ByteSize,
	pub partitions: usize,
	pub intervals: usize,
	pub entries: usize,
	pub counters: RangeMetrics,
}

type BucketCounters = Box<[RangeMetrics]>;

#[cfg(test)]
pub(crate) type MaterializeInterlock<D> = Box<dyn Fn(&RangeTier<D>, <D as RangeDomain>::Partition) + Send + Sync>;

#[cfg(test)]
pub(crate) type ServeInterlock<D> = Box<dyn Fn(&RangeTier<D>) + Send + Sync>;

struct PoolInner<D: RangeDomain> {
	shards: Box<[Mutex<Shard<D>>]>,
	coverage: RwLock<CoverageIndex<D::Dimension, D::Key>>,
	retractions: Retractions,
	gap_guard: usize,
	#[cfg(test)]
	interlock: Option<MaterializeInterlock<D>>,
	#[cfg(test)]
	serve_interlock: Option<ServeInterlock<D>>,
}

pub struct RangeTier<D: RangeDomain> {
	inner: Arc<PoolInner<D>>,
}

impl<D: RangeDomain> Clone for RangeTier<D> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}
