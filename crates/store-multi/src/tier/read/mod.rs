// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the multi-version store, caching keys the commit buffer has evicted so a repeated
//! point read need not fall through to persistent every time. The previous slot is only ever filled by an
//! in-place supersede, so it stays version-adjacent to the current slot. Range scans consult this tier only
//! for `range_complete` buckets, and the always-scanned commit buffer still wins on version, so the cache
//! can never mask a newer value nor resurrect a deleted one.

mod point;
mod pool;
mod range;
#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU8},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
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

struct ResidentPage {
	entries: BTreeMap<EncodedKey, PageEntry>,
	bytes: usize,
	payload: usize,
	hot: bool,
	tick: u64,
	range_complete: bool,
	warm_blocked: bool,
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
pub struct ReadBufferWarmMetrics {
	pub warms_started: u64,
	pub warms_completed: u64,
	pub warms_dirty_aborted: u64,
	pub warms_aborted: u64,
	pub pages_warm_blocked: u64,
	pub pages_evicted: u64,
	pub complete_pages_invalidated: u64,
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
	pub blocked_pages: usize,
	pub warming: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferShardMetrics {
	pub shard: usize,
	pub state: ReadBufferStateMetrics,
	pub warms: ReadBufferWarmMetrics,
	pub reads: ReadBufferReadMetrics,
}

struct Shard {
	pages: HashMap<PageId, ResidentPage>,
	warming: HashMap<PageId, bool>,
	next_tick: u64,
	page_cap: usize,
	budget: MemoryBudget,
	warm_metrics: ReadBufferWarmMetrics,
	read_metrics: ReadBufferReadMetrics,
}

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	bucket_shift: AtomicU8,
	enabled: AtomicBool,
}

#[derive(Clone)]
pub struct MultiReadBufferTier {
	inner: Arc<PoolInner>,
}
