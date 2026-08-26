// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Point tier: a single-version cache of point reads that also remembers absences, so a key read many
//! times costs one persistent lookup rather than one per read. An entry keys on the whole key under its
//! dimension instead of on a bucket, so neighbouring keys spread across every shard and eviction removes
//! one entry rather than every key that happened to share a prefix.
//!
//! Which counter slot a key belongs to and which of those slots this tier may hold is the domain's to
//! answer, through [`PointDomain`]; a key too short to name a slot is declined rather than cached. A slot
//! the domain keeps out is answered from the key alone, before any hash or lock, and its miss is charged
//! to a lock free per slot counter that folds into the slot table.

#[cfg(test)]
mod domain;
#[allow(clippy::module_inception)]
mod point;
mod pool;
#[cfg(test)]
mod tests;

use std::{
	borrow::Cow,
	collections::HashMap,
	fmt::Debug,
	hash::Hash,
	mem::size_of,
	sync::{Arc, atomic::AtomicU64},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::util::budget::MemoryBudget;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::RowBytes;

/// How one keyspace shape is cut into counter slots, and which of them this tier may hold.
///
/// The tier never decodes a key itself: every question that depends on what a key means is answered
/// here, so the same machinery serves stores whose keys share nothing.
pub trait PointDomain: Copy + Debug + 'static {
	type Dimension: Copy + Eq + Hash + Send + Sync + 'static;
	type Slot: Copy + Eq + Debug + Send + Sync + 'static;
	/// What this tier stores per key; anything a reader filters on must survive here, or the cache answers wrong.
	type Row: RowBytes + Clone + Send + Sync + 'static;

	/// The number of counter slots, which must bound every index [`PointDomain::slot`] returns.
	const SLOTS: usize;

	const SCOPE: &'static str;

	/// The slot this key belongs to, or `None` for a key too short to name one; a declined key is
	/// never admitted, since the tier could not attribute it.
	fn slot(key: &EncodedKey) -> Option<usize>;

	/// Whether the slot may live in this tier. One the domain keeps out is refused on every admission
	/// path, and its read is answered before a shard is hashed or locked.
	fn caches_points(slot: usize) -> bool;

	fn slot_at(index: usize) -> Self::Slot;

	fn slot_name(slot: Self::Slot) -> Cow<'static, str>;
}

#[derive(Clone, Copy, Debug)]
pub struct PointConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for PointConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			shards: 16,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PointKey<K> {
	pub dimension: K,
	pub key: EncodedKey,
}

struct Entry<D: PointDomain> {
	key: PointKey<D::Dimension>,
	row: Option<D::Row>,
	tick: u64,
}

const INDEX_FILL_DIVISOR: usize = 2;

const fn entry_overhead<D: PointDomain>() -> usize {
	size_of::<Entry<D>>() + INDEX_FILL_DIVISOR * (size_of::<PointKey<D::Dimension>>() + size_of::<usize>())
}

#[cfg(test)]
const ENTRY_OVERHEAD: usize = entry_overhead::<domain::TestDomain>();

const EVICTION_SAMPLE: usize = 8;

fn entry_footprint<D: PointDomain>(key: &PointKey<D::Dimension>, row: &Option<D::Row>) -> usize {
	entry_overhead::<D>() + key.key.heap_bytes() + row.as_ref().map_or(0, RowBytes::row_bytes)
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
pub struct PointSlotMetrics<D: PointDomain> {
	pub slot: D::Slot,
	pub used: ByteSize,
	pub entries: usize,
	pub counters: PointMetrics,
}

type SlotCounters = Box<[PointMetrics]>;

struct Shard<D: PointDomain> {
	index: HashMap<PointKey<D::Dimension>, usize>,
	entries: Vec<Entry<D>>,
	filling: HashMap<PointKey<D::Dimension>, bool>,
	budget: MemoryBudget,
	next_tick: u64,
	rng: u64,
	metrics: PointMetrics,
	slot_metrics: SlotCounters,
}

#[cfg(test)]
pub(crate) type FillInterlock<D> = Box<dyn Fn(&PointTier<D>, &PointKey<<D as PointDomain>::Dimension>) + Send + Sync>;

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
