// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Range tier: a partial-coverage cache over a keyed store, where the unit of proof is the interval
//! between two adjacent observed keys rather than a whole key prefix.
//!
//! Rows live in per-partition maps behind the existing shard mutexes; the claim that RAM is
//! authoritative over a span lives apart, in one ordered coverage index keyed by dimension. What a
//! partition is, which of them may be cached and how a key maps onto one is the domain's to answer,
//! through [`RangeDomain`]; a key too short to name a partition is declined rather than cached.
//!
//! The two structures are never locked together, so every path is a sequence of separately locked
//! steps. They are ordered so that coverage understates what RAM holds rather than overstating it:
//!
//! ```text
//! materialize            insert rows first, then extend coverage
//! remove or invalidate   shrink coverage first, then drop rows
//! evict                  shrink coverage first, then drop rows
//! read                   observe coverage first, then read the row
//! ```
//!
//! Overstating answers a key the persistent tier still holds as a proven absence, which is silent
//! wrong data; understating costs one persistent read. The two locks are never held together: the
//! write paths and the materialize path take them in opposite directions.
//!
//! Two claims survive a write. A removal becomes a `Deleted` entry, so the interval around it stays
//! authoritative until the flush demotes it to `Absent`; that `Absent` entry must not vanish, or a
//! scan still in flight reinstates the row it read before the flush. A write landing in an uncovered
//! gap becomes a one-key island, since the writer knows nothing about the neighbourhood.

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

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::util::{budget::MemoryBudget, sorted::SortedVecMap};
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_value::byte_size::ByteSize;

use crate::coverage::{
	ExclusiveUpperEnd,
	entry::{Entry, PinnedCount},
	index::CoverageIndex,
	interval::Interval,
	plan::{DEFAULT_GAP_GUARD, GapHistogram, Segment},
	retraction::Retractions,
	successor,
};

/// The heap cost of one cached row, so the tier can charge a row type it never inspects.
pub trait RowBytes {
	fn row_bytes(&self) -> usize;
}

impl RowBytes for EncodedPodRow {
	fn row_bytes(&self) -> usize {
		self.len()
	}
}

/// How one keyspace shape is cut into partitions, and which of them this tier may hold.
///
/// The tier never decodes a key itself: every question that depends on what a key means is answered
/// here, so the same machinery serves stores whose keys share nothing.
pub trait RangeDomain: Copy + Debug + 'static {
	type Dimension: Copy + Eq + Hash + Send + Sync + 'static;
	type Partition: Copy + Eq + Hash + Send + Sync + 'static;
	type Slot: Copy + Eq + Debug + Send + Sync + 'static;
	/// What this tier stores per key; anything a reader filters on must survive here, or the cache answers wrong.
	type Row: RowBytes + Clone + Send + Sync + 'static;

	/// The shortest key that still names a partition; a shorter one is declined.
	const PREFIX_LEN: usize;

	/// The number of counter slots, which must bound every index [`RangeDomain::slot`] returns.
	const SLOTS: usize;

	const SCOPE: &'static str;

	const GAP_SCOPE: &'static str;

	fn partition(dimension: Self::Dimension, key: &EncodedKey) -> Option<Self::Partition>;

	fn dimension(partition: &Self::Partition) -> Self::Dimension;

	/// The half-open span this partition owns, so a whole partition retracts in one shrink.
	fn span(partition: &Self::Partition) -> (EncodedKey, ExclusiveUpperEnd);

	/// The inclusive band a head may prove empty, or `None` where the domain proves no such absence.
	fn head_band(dimension: Self::Dimension) -> Option<(EncodedKey, EncodedKey)>;

	/// Whether the partition may live in this tier. One the domain keeps out is never materialized,
	/// and its gaps never count against the gap guard, or a wide scan would degrade forever.
	fn caches_ranges(partition: &Self::Partition) -> bool;

	/// The end of the run of adjacent partitions sharing this one's admission answer, so a gap
	/// splits once per run rather than once per partition.
	fn policy_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd;

	/// Whether `incoming` may replace the resident row; a domain that versions its rows must refuse a downgrade.
	fn supersedes(_resident: &Self::Row, _incoming: &Self::Row) -> bool {
		true
	}

	/// A handoff domain must seat a write no claim reached, or a claim taken across it answers the row absent.
	fn admits_unproven_writes() -> bool {
		false
	}

	fn slot(partition: &Self::Partition) -> usize;

	fn slot_at(index: usize) -> Self::Slot;

	fn slot_name(slot: Self::Slot) -> Cow<'static, str>;
}

#[derive(Clone, Copy, Debug)]
pub struct RangeConfig {
	pub shard_bytes: Option<ByteSize>,
	pub shards: usize,
	/// Non-exempt gaps a plan may carry before it is abandoned for one full scan; a plan of many
	/// small persistent reads is slower than no cache at all.
	pub gap_guard: usize,
}

impl RangeConfig {
	/// A budget for tests only; production sizing comes from catalog config, never from a fallback here.
	pub fn testing() -> Self {
		Self {
			shard_bytes: Some(ByteSize::from_mib(4)),
			shards: 16,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}
}

/// The exclusive upper end of a prefix, or `None` when the prefix is all `0xff` and has none.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}

pub type RangeRows<D> = Vec<(EncodedKey, <D as RangeDomain>::Row)>;

/// The key range a caller reads from the persistent tier to fill one gap segment.
pub fn scan_range(gap: &Interval) -> EncodedKeyRange {
	let end = match &gap.end {
		ExclusiveUpperEnd::Key(key) => Bound::Excluded(key.clone()),
		ExclusiveUpperEnd::Top => Bound::Unbounded,
	};
	EncodedKeyRange::new(Bound::Included(gap.start.clone()), end)
}

/// The span a persistent read proved, given where it stopped.
///
/// An exhausted read proves the whole gap. A read that stopped early proves only up to and
/// including its last key, so the claim ends at that key's successor; ending it at the key itself
/// would leave the key uncovered, and ending it at the gap end would claim a span the read never
/// reached. A read that returned nothing without exhausting proves nothing.
pub fn proven_span(gap: &Interval, last_key: Option<&EncodedKey>, exhausted: bool) -> Option<Interval> {
	if exhausted {
		return Some(gap.clone());
	}
	let last = last_key?;
	Some(Interval::new(gap.start.clone(), ExclusiveUpperEnd::Key(successor(last)).min(gap.end.clone())))
}

struct Partition<R> {
	entries: SortedVecMap<EncodedKey, Entry<R>>,
	/// Entries eviction may not drop, so the budget can stop instead of spinning on a partition
	/// whose every entry carries an unflushed removal.
	pinned: PinnedCount,
	bytes: usize,
	tick: u64,
	/// Bumped by every materialize: a move between choosing a victim and dropping its rows means a
	/// materialize claimed the span, and dropping the rows would leave that claim standing over nothing.
	materializes: u64,
	/// The shard write counter at the last write here; a local tally would reset when a partition is reseated.
	written_at: u64,
	/// Whether this partition has ever taken part in a claim; it gates the coverage lock for every
	/// write and must err toward `true`, since a false no leaves a stale claim standing.
	covered: bool,
}

/// What eviction saw when it chose a victim; either count moving means another caller has claimed since.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Progress {
	materializes: u64,
	written_at: u64,
}

impl<R> Partition<R> {
	fn progress(&self) -> Progress {
		Progress {
			materializes: self.materializes,
			written_at: self.written_at,
		}
	}
}

struct Shard<D: RangeDomain> {
	partitions: HashMap<D::Partition, Partition<D::Row>>,
	budget: MemoryBudget,
	next_tick: u64,
	/// Bumped by every landed write, so a rollback can tell whether the row it drops is still its own.
	writes: u64,
	gaps: GapHistogram,
	metrics: RangeMetrics,
	slot_metrics: SlotCounters,
}

const fn entry_overhead<R>() -> usize {
	size_of::<(EncodedKey, Entry<R>)>()
}

#[cfg(test)]
const ENTRY_OVERHEAD: usize = entry_overhead::<EncodedPodRow>();

const fn partition_overhead<D: RangeDomain>() -> usize {
	size_of::<D::Partition>() + size_of::<Partition<D::Row>>()
}

fn entry_footprint<R: RowBytes>(key: &EncodedKey, entry: &Entry<R>) -> usize {
	entry_overhead::<R>() + key.heap_bytes() + entry.value().map_or(0, RowBytes::row_bytes)
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

/// A planned scan, and the token its materializes use to prove they did not race a retraction.
///
/// The token is the retraction count read at plan time, bumped by every coverage shrink; without it
/// a materialize would reinstate a claim over rows a concurrent write had already removed.
pub struct RangeScan<D: RangeDomain> {
	pub(super) dimension: D::Dimension,
	pub(super) advanced: bool,
	pub(super) segments: Vec<Segment>,
	pub(super) gaps: usize,
	pub(super) degraded: bool,
	pub(super) retractions: u64,
}

/// What a materialize did with the span it was handed.
///
/// `NothingCacheable` and `Refused` both leave RAM unchanged, but only `Refused` means the tier
/// declined the claim; a caller that stops materializing on `NothingCacheable` starves every partition
/// sitting behind an uncacheable one for the rest of the scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Materialize {
	Materialized,
	NothingCacheable,
	Refused,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeMetrics {
	/// Segments a plan answered from RAM.
	pub hits: u64,
	/// Non-exempt gaps a plan handed to the persistent tier.
	pub misses: u64,
	/// Gaps the domain exempts, served by neither this tier nor the persistent one.
	pub exempt: u64,
	/// Spans a persistent read proved and this tier took.
	pub materializes: u64,
	/// Spans refused whole because they did not fit the shard budget.
	pub materializes_refused: u64,
	/// Spans refused because a claim was withdrawn while the persistent read was in flight.
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
pub struct RangeSlotMetrics<D: RangeDomain> {
	pub slot: D::Slot,
	pub used: ByteSize,
	pub partitions: usize,
	pub intervals: usize,
	pub entries: usize,
	pub counters: RangeMetrics,
}

type SlotCounters = Box<[RangeMetrics]>;

#[cfg(test)]
pub(crate) type MaterializeInterlock<D> = Box<dyn Fn(&RangeTier<D>, <D as RangeDomain>::Partition) + Send + Sync>;

#[cfg(test)]
pub(crate) type ServeInterlock<D> = Box<dyn Fn(&RangeTier<D>) + Send + Sync>;

struct PoolInner<D: RangeDomain> {
	shards: Box<[Mutex<Shard<D>>]>,
	coverage: RwLock<CoverageIndex<D::Dimension>>,
	/// Bumped inside every coverage shrink, so a reader and a materialize can each tell that no claim
	/// was withdrawn between two of their steps.
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
