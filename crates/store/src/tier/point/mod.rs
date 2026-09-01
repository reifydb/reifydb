// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod domain;
#[allow(clippy::module_inception)]
mod point;
mod pool;
#[cfg(test)]
mod tests;

use std::{
	borrow::Cow,
	collections::{HashMap, hash_map::DefaultHasher},
	fmt::Debug,
	hash::{Hash, Hasher},
	mem::size_of,
	sync::{Arc, atomic::AtomicU64},
};

use hashbrown::HashTable;
use reifydb_core::{key::typed::Key, metrics::heap::HeapSize, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::RowBytes;

pub trait PointDomain: Copy + Debug + 'static {
	type Dimension: Copy + Eq + Hash + Send + Sync + 'static;
	type Key: Key;
	type MetricBucket: Copy + Eq + Debug + Send + Sync + 'static;
	type Row: RowBytes + Clone + Send + Sync + 'static;

	const METRIC_BUCKETS: usize;

	const SCOPE: &'static str;

	fn metric_bucket(key: &Self::Key) -> Option<usize>;

	fn caches_points(bucket: usize) -> bool;

	fn supersede(resident: &mut Self::Row, incoming: Self::Row) -> bool {
		*resident = incoming;
		true
	}

	fn metric_bucket_at(index: usize) -> Self::MetricBucket;

	fn metric_bucket_name(bucket: Self::MetricBucket) -> Cow<'static, str>;
}

#[derive(Clone, Copy, Debug)]
pub struct PointConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl PointConfig {
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(ByteSize::from_mib(4)),
			shards: 16,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PointKey<D, K> {
	pub dimension: D,
	pub key: K,
}

struct Entry<D: PointDomain> {
	key: PointKey<D::Dimension, D::Key>,
	row: Option<D::Row>,
	tick: u64,
}

const APPROXIMATED_INDEX_OVERHEAD: usize = 6;

const fn entry_overhead<D: PointDomain>() -> usize {
	size_of::<Entry<D>>() + APPROXIMATED_INDEX_OVERHEAD
}

#[cfg(test)]
const ENTRY_OVERHEAD: usize = entry_overhead::<domain::TestDomain>();

const EVICTION_SAMPLE: usize = 8;

fn entry_footprint<D: PointDomain>(key: &PointKey<D::Dimension, D::Key>, row: &Option<D::Row>) -> usize {
	entry_overhead::<D>() + key.key.heap_size() + row.as_ref().map_or(0, RowBytes::row_bytes)
}

const BUCKET_SEED: u64 = 0xD6E8_FEB8_6659_FD93;

fn bucket_hash<D: Hash, K: Hash>(dimension: &D, key: &K) -> u64 {
	let mut hasher = DefaultHasher::new();
	hasher.write_u64(BUCKET_SEED);
	dimension.hash(&mut hasher);
	key.hash(&mut hasher);
	hasher.finish()
}

fn entry_hash<D: PointDomain>(entry: &Entry<D>) -> u64 {
	bucket_hash(&entry.key.dimension, &entry.key.key)
}

fn find_position<D: PointDomain>(shard: &Shard<D>, hash: u64, dimension: D::Dimension, key: &D::Key) -> Option<u32> {
	let Shard {
		index,
		entries,
		..
	} = shard;
	index.find(hash, |position| {
		let entry = &entries[*position as usize];
		entry.key.dimension == dimension && entry.key.key == *key
	})
	.copied()
}

fn account(budget: &MemoryBudget, old: usize, new: usize) {
	if new >= old {
		budget.charge(ByteSize::from_bytes((new - old) as u64));
	} else {
		budget.release(ByteSize::from_bytes((old - new) as u64));
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PointMetrics {
	pub hits: u64,
	pub misses: u64,
	pub insertions: u64,
	pub evictions: u64,
	pub fills_started: u64,
	pub fills_dirty_aborted: u64,
	pub fills_duplicate: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct PointShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub entries: usize,
	pub counters: PointMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct PointBucketMetrics<D: PointDomain> {
	pub bucket: D::MetricBucket,
	pub used: ByteSize,
	pub entries: usize,
	pub counters: PointMetrics,
}

type BucketCounters = Box<[PointMetrics]>;

struct Shard<D: PointDomain> {
	index: HashTable<u32>,
	entries: Vec<Entry<D>>,
	filling: HashMap<PointKey<D::Dimension, D::Key>, bool>,
	budget: MemoryBudget,
	next_tick: u64,
	rng: u64,
	metrics: PointMetrics,
	bucket_metrics: BucketCounters,
}

#[cfg(test)]
pub(crate) type FillInterlock<D> =
	Box<dyn Fn(&PointTier<D>, &PointKey<<D as PointDomain>::Dimension, <D as PointDomain>::Key>) + Send + Sync>;

struct PoolInner<D: PointDomain> {
	shards: Box<[Mutex<Shard<D>>]>,
	excluded_misses: Box<[AtomicU64]>,
	#[cfg(test)]
	interlock: Option<FillInterlock<D>>,
}

pub struct PointTier<D: PointDomain> {
	inner: Arc<PoolInner<D>>,
}

impl<D: PointDomain> Clone for PointTier<D> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}
