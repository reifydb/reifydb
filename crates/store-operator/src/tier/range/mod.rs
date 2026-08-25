// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Range tier of the operator store: a partial-coverage cache over operator state, where the unit
//! of proof is the interval between two adjacent observed keys rather than a whole key prefix.
//!
//! Rows live in per-partition maps behind the existing shard mutexes; the claim that RAM is
//! authoritative over a span lives apart, in one ordered coverage index keyed by operator. A
//! partition is the set of keys sharing one `(operator, group, keyspace)` prefix, and a key too
//! short to carry both a group and a keyspace is declined rather than cached.
//!
//! The two structures are never locked together, so every path is a sequence of separately locked
//! steps. They are ordered so that coverage understates what RAM holds rather than overstating it:
//!
//! ```text
//! install                insert rows first, then extend coverage
//! remove or invalidate   shrink coverage first, then drop rows
//! evict                  shrink coverage first, then drop rows
//! read                   observe coverage first, then read the row
//! ```
//!
//! Overstating answers a key the persistent tier still holds as a proven absence, which is silent
//! wrong data; understating costs one persistent read. The two locks are never held together: the
//! write paths and the install path take them in opposite directions.
//!
//! Two claims survive a write. A removal becomes a `Deleted` entry, so the interval around it stays
//! authoritative until the flush demotes it to `Absent`; that `Absent` entry must not vanish, or a
//! scan still in flight reinstates the row it read before the flush. A write landing in an uncovered
//! gap becomes a one-key island, since the writer knows nothing about the neighbourhood.

mod point;
mod pool;
mod scan;
#[cfg(test)]
mod tests;
mod write;

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	ops::Bound,
	sync::Arc,
};

use reifydb_codec::{
	key::{
		decode_u64, encode_u8,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_store::coverage::{
	Edge,
	entry::{Entry, PinnedCount},
	interval::{CoverageSet, Interval},
	plan::{DEFAULT_GAP_GUARD, GapHistogram, Segment},
	retraction::Retractions,
	successor,
};
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
	/// Non-exempt gaps a plan may carry before it is abandoned for one full scan; a plan of many
	/// small persistent reads is slower than no cache at all.
	pub gap_guard: usize,
}

impl Default for OperatorRangeConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			shards: 16,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}
}

/// The set of keys sharing one `(operator, group, keyspace)` prefix: the unit of row storage,
/// sharding, budgeting and eviction.
///
/// Coverage is not partitioned. Two claims over adjacent partitions coalesce into one interval,
/// which is why eviction retracts a partition's whole span rather than a named interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PartitionId {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keyspace: Keyspace,
}

impl PartitionId {
	pub const PREFIX_LEN: usize = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize + 1;

	/// Declines a key shorter than the prefix: the tier cannot tell which claim it would belong
	/// to, so it must never be cached.
	pub fn of(operator: OperatorId, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		let group = GroupId(decode_u64(bytes[..offset].try_into().ok()?));
		Some(Self {
			operator,
			group,
			keyspace: Keyspace(encode_u8(bytes[offset])),
		})
	}

	/// The encoded `group || keyspace` prefix, which must round-trip [`PartitionId::of`].
	pub fn prefix(&self) -> EncodedKey {
		EncodedKey::new(OperatorStateKey::inner_encoded(self.group, self.keyspace, [0u8; 0]).as_bytes())
	}

	/// The half-open span this partition owns, so a whole partition retracts in one shrink.
	pub fn span(&self) -> (EncodedKey, Edge) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => Edge::of(successor),
			None => Edge::Top,
		};
		(start, end)
	}

	/// Whether the keyspace may live in this tier. A partition the policy keeps out is never
	/// installed, and its gaps never count against the gap guard, or a group-wide scan would degrade
	/// forever.
	pub fn caches_ranges(&self) -> bool {
		self.keyspace.cache_policy().caches_ranges()
	}
}

/// The exclusive upper end of a prefix, or `None` when the prefix is all `0xff` and has none.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}

pub type RangeRows = Vec<(EncodedKey, EncodedPodRow)>;

/// The key range a caller reads from the persistent tier to fill one gap segment.
pub fn scan_range(gap: &Interval) -> EncodedKeyRange {
	let end = match &gap.end {
		Edge::Key(key) => Bound::Excluded(key.clone()),
		Edge::Top => Bound::Unbounded,
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
	Some(Interval::new(gap.start.clone(), Edge::Key(successor(last)).min(gap.end.clone())))
}

struct Partition {
	entries: BTreeMap<EncodedKey, Entry<EncodedPodRow>>,
	/// Entries eviction may not drop, so the budget can stop instead of spinning on a partition
	/// whose every entry carries an unflushed removal.
	pinned: PinnedCount,
	bytes: usize,
	tick: u64,
	/// Bumped by every install: a move between choosing a victim and dropping its rows means an
	/// install claimed the span, and dropping the rows would leave that claim standing over nothing.
	installs: u64,
	/// Whether this partition has ever taken part in a claim; it gates the coverage lock for every
	/// write and must err toward `true`, since a false no leaves a stale claim standing.
	covered: bool,
}

struct Shard {
	partitions: HashMap<PartitionId, Partition>,
	budget: MemoryBudget,
	next_tick: u64,
	gaps: GapHistogram,
	metrics: OperatorRangeMetrics,
	keyspace_metrics: KeyspaceCounters,
}

/// The claims RAM holds, one disjoint coalesced set per operator, behind a single lock.
struct CoverageIndex {
	operators: HashMap<OperatorId, CoverageSet>,
}

const NODE_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = NODE_FILL_DIVISOR * (size_of::<EncodedKey>() + size_of::<Entry<EncodedPodRow>>());

const PARTITION_OVERHEAD: usize = size_of::<PartitionId>() + size_of::<Partition>();

fn entry_footprint(key: &EncodedKey, entry: &Entry<EncodedPodRow>) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + entry.value().map_or(0, |row| row.len())
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

/// A planned scan, and the token its installs use to prove they did not race a retraction.
///
/// The token is the retraction count read at plan time, bumped by every coverage shrink; without it
/// an install would reinstate a claim over rows a concurrent write had already removed.
pub struct RangeScan {
	pub(super) operator: OperatorId,
	pub(super) segments: Vec<Segment>,
	pub(super) gaps: usize,
	pub(super) degraded: bool,
	pub(super) retractions: u64,
}

/// What an install did with the span it was handed.
///
/// `NothingCacheable` and `Refused` both leave RAM unchanged, but only `Refused` means the tier
/// declined the claim; a caller that stops installing on `NothingCacheable` starves every keyspace
/// sitting behind an uncacheable one for the rest of the scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Install {
	Installed,
	NothingCacheable,
	Refused,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorRangeMetrics {
	/// Segments a plan answered from RAM.
	pub hits: u64,
	/// Non-exempt gaps a plan handed to the persistent tier.
	pub misses: u64,
	/// Spans a persistent read proved and this tier took.
	pub installs: u64,
	/// Spans refused whole because they did not fit the shard budget.
	pub installs_refused: u64,
	/// Spans refused because a claim was withdrawn while the persistent read was in flight.
	pub installs_raced: u64,
	pub evictions: u64,
	pub point_hits: u64,
	pub point_misses: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub partitions: usize,
	pub entries: usize,
	pub counters: OperatorRangeMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeKeyspaceMetrics {
	pub keyspace: Keyspace,
	pub used: ByteSize,
	pub partitions: usize,
	pub intervals: usize,
	pub entries: usize,
	pub counters: OperatorRangeMetrics,
}

const KEYSPACE_SLOTS: usize = 256;

type KeyspaceCounters = Box<[OperatorRangeMetrics; KEYSPACE_SLOTS]>;

#[cfg(test)]
pub(crate) type InstallInterlock = Box<dyn Fn(&OperatorRangeTier, PartitionId) + Send + Sync>;

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	coverage: RwLock<CoverageIndex>,
	/// Bumped inside every coverage shrink, so a reader and an install can each tell that no claim
	/// was withdrawn between two of their steps.
	retractions: Retractions,
	gap_guard: usize,
	#[cfg(test)]
	interlock: Option<InstallInterlock>,
}

#[derive(Clone)]
pub struct OperatorRangeTier {
	inner: Arc<PoolInner>,
}
