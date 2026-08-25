// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the multi-version store, caching keys the commit buffer has evicted so a repeated
//! point read need not fall through to persistent every time. The previous slot is only ever filled by an
//! in-place supersede, so it stays version-adjacent to the current slot. The always-scanned commit buffer
//! still wins on version, so the cache can never mask a newer value nor resurrect a deleted one.
//!
//! Residency is recorded in [`coverage`]: the shared interval model, which claims spans proven by what a
//! fill actually placed rather than whole buckets. A forward range read consults it and walks the pages one
//! claim spans; where no claim covers the resume point the read falls through to the persistent tier.

mod coverage;
mod point;
mod pool;
#[cfg(test)]
mod race;
mod range;
mod scan;
#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	sync::{Arc, atomic::AtomicU64},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind, util::budget::MemoryBudget};
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_store::{
	coverage::{Edge, interval::CoverageSet},
	row::page::{DEFAULT_BUCKET_SHIFT, PageId},
};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::tier::RangeBatch;

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferConfig {
	pub resident_pages: usize,
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
	pub bucket_shift: u8,
}

impl Default for ReadBufferConfig {
	fn default() -> Self {
		Self {
			resident_pages: 1024,
			resident_bytes: Some(ByteSize::from_gib(2)),
			shards: 16,
			bucket_shift: DEFAULT_BUCKET_SHIFT,
		}
	}
}

#[derive(Clone)]
struct PageEntry {
	version: CommitVersion,
	value: Option<CowVec<u8>>,
	previous: Option<(CommitVersion, Option<CowVec<u8>>)>,
}

/// The half-open span of one claim, and the unit a page's hull is widened by.
type Span = (EncodedKey, Edge);

struct ResidentPage {
	entries: BTreeMap<EncodedKey, PageEntry>,
	bytes: usize,
	payload: usize,
	hot: bool,
	tick: u64,
	/// The union hull of every claim this page has published, so a page leaves the tier in one shrink
	/// even for a kind whose bucket has no reconstructable key range.
	///
	/// `page_of` maps a key to exactly one page and distinct pages never share a byte domain, so a
	/// hull can never reach a key another page is responsible for.
	claimed: Option<Span>,
	/// The tier-wide sequence number the last claim-bearing fill on this page drew, or zero when no
	/// such fill has run; a move between choosing a victim and dropping it means a fill published a
	/// claim, and dropping the page would leave that claim standing over nothing.
	///
	/// The number must come from one monotonic tier counter and never restart per page: two evictors
	/// can hold one victim, and a page dropped by the first and recreated by a fill would otherwise
	/// present the value the second already read, so its stale drop would take a page it never
	/// inspected. Zero is safe to repeat because a page that never drew a number never claimed.
	fills: u64,
}

impl ResidentPage {
	fn fresh(tick: u64) -> Self {
		Self {
			entries: BTreeMap::new(),
			bytes: 0,
			payload: 0,
			hot: false,
			tick,
			claimed: None,
			fills: 0,
		}
	}
}

/// The claims RAM holds, one disjoint coalesced set per entry kind, behind a single lock.
///
/// `EntryKind` is the dimension a range read is already parameterised by, so one scan consults one
/// set, and every key of one kind sorts together in the encoded key space.
struct CoverageIndex {
	kinds: HashMap<EntryKind, CoverageSet>,
}

const NODE_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = NODE_FILL_DIVISOR * (size_of::<EncodedKey>() + size_of::<PageEntry>());

fn value_len(value: &Option<CowVec<u8>>) -> usize {
	value.as_ref().map_or(0, |bytes| bytes.len())
}

#[derive(Clone, Copy, Default)]
struct EntryFootprint {
	resident: usize,
	payload: usize,
}

fn entry_footprint(key: &EncodedKey, entry: &PageEntry) -> EntryFootprint {
	let version_payload = key.len() + size_of::<CommitVersion>();
	EntryFootprint {
		resident: ENTRY_OVERHEAD
			+ key.heap_bytes() + value_len(&entry.value)
			+ entry.previous.as_ref().map_or(0, |(_, value)| value_len(value)),
		payload: version_payload
			+ value_len(&entry.value)
			+ entry.previous.as_ref().map_or(0, |(_, value)| version_payload + value_len(value)),
	}
}

fn account(bytes: &mut usize, payload: &mut usize, budget: &MemoryBudget, old: EntryFootprint, new: EntryFootprint) {
	if new.resident >= old.resident {
		let delta = new.resident - old.resident;
		*bytes += delta;
		budget.charge(ByteSize::from_bytes(delta as u64));
	} else {
		let delta = old.resident - new.resident;
		*bytes -= delta;
		budget.release(ByteSize::from_bytes(delta as u64));
	}
	if new.payload >= old.payload {
		*payload += new.payload - old.payload;
	} else {
		*payload -= old.payload - new.payload;
	}
}

pub enum ServedChunk {
	Served(RangeBatch),
	Gap,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadBufferPageMetrics {
	pub pages_evicted: u64,
	pub complete_pages_invalidated: u64,
}

/// How one refused attempt to serve a forward chunk from the interval coverage ended.
enum CoverageOutcome {
	Gap,
	Refused,
}

/// Outcomes of the interval serve, counted apart from `range_served` so a refused plan stays
/// distinguishable from a span no claim reached.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadBufferCoverageMetrics {
	pub served: u64,
	pub rows: u64,
	pub gaps: u64,
	pub refused: u64,
	pub installs: u64,
	pub install_rows: u64,
	pub installs_refused: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadBufferReadMetrics {
	pub point_hits: u64,
	pub previous_hits: u64,
	pub point_misses: u64,
	pub range_served: u64,
	pub range_gaps: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferStateMetrics {
	pub used: ByteSize,
	pub limit: ByteSize,
	pub pages: usize,
	pub page_cap: usize,
	pub payload: ByteSize,
	pub entries: usize,
	pub hot_pages: usize,
	pub complete_pages: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferShardMetrics {
	pub shard: usize,
	pub state: ReadBufferStateMetrics,
	pub pages: ReadBufferPageMetrics,
	pub reads: ReadBufferReadMetrics,
	pub coverage: ReadBufferCoverageMetrics,
}

struct Shard {
	pages: HashMap<PageId, ResidentPage>,
	next_tick: u64,
	page_cap: usize,
	budget: MemoryBudget,
	page_metrics: ReadBufferPageMetrics,
	read_metrics: ReadBufferReadMetrics,
	coverage_metrics: ReadBufferCoverageMetrics,
}

#[cfg(test)]
type FillInterlock = Box<dyn Fn(&MultiReadBufferTier, PageId) + Send + Sync>;
#[cfg(test)]
type InvalidateInterlock = Box<dyn Fn(&MultiReadBufferTier, &EncodedKey) + Send + Sync>;

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	bucket_shift: u8,
	coverage: RwLock<CoverageIndex>,
	/// Bumped inside every coverage shrink, so a fill can tell that no claim was withdrawn between
	/// reading the token and publishing.
	retractions: AtomicU64,
	/// Drawn by every claim-bearing fill and never reused, so a page dropped and refilled can never
	/// present the value a stale evictor sampled before the drop.
	fill_sequence: AtomicU64,
	#[cfg(test)]
	claims_published: AtomicU64,
	#[cfg(test)]
	claims_refused: AtomicU64,
	#[cfg(test)]
	drops_refused: AtomicU64,
	#[cfg(test)]
	interlock: Option<FillInterlock>,
	/// Entered while an invalidate holds neither lock, so a test can read the tier at the one instant a
	/// claim that outlived its row would be visible to every other thread.
	#[cfg(test)]
	invalidate_interlock: Option<InvalidateInterlock>,
}

#[derive(Clone)]
pub struct MultiReadBufferTier {
	inner: Arc<PoolInner>,
}
