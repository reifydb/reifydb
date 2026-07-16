// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the multi-version store. Serves cold keys that the commit buffer has already evicted below
//! the eviction watermark, so a repeated point read does not have to fall through to the persistent tier every
//! time. Each entry caches the latest committed `(version, value)` plus, while that version is still unflushed,
//! the immediately superseded one; a hit is served from the newest slot at or below the requested snapshot
//! version, otherwise the caller reads through to the persistent tier which honors the full version bound. The
//! previous slot is only ever filled by an in-place supersede (never by a warm merge), so it is guaranteed to be
//! version-adjacent to the current slot. Range scans consult this tier only for buckets marked `range_complete`: a
//! whole page loaded in one consistent read of the persistent tier, which therefore mirrors every persisted row for
//! its contiguous key interval and can serve the persistent contribution of a range scan. Any incomplete bucket
//! reads through to the persistent tier, and the always-scanned commit buffer still wins on version, so the cache
//! can never mask a newer value nor resurrect a deleted one.

mod point;
mod pool;
mod range;
#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	sync::{Arc, atomic::AtomicU8},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::tier::RangeBatch;

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferDomainConfig {
	pub resident_pages: usize,
	pub resident_bytes: ByteSize,
	pub shards: usize,
}

impl Default for ReadBufferDomainConfig {
	fn default() -> Self {
		Self {
			resident_pages: 1024,
			resident_bytes: ByteSize::from_mib(256),
			shards: 16,
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferConfig {
	pub operator: ReadBufferDomainConfig,
	pub general: ReadBufferDomainConfig,
	pub bucket_shift: u8,
}

impl Default for ReadBufferConfig {
	fn default() -> Self {
		let domain = ReadBufferDomainConfig {
			resident_bytes: ByteSize::from_gib(2),
			..ReadBufferDomainConfig::default()
		};
		Self {
			operator: domain,
			general: domain,
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

const ENTRY_OVERHEAD: usize = size_of::<EncodedKey>() + size_of::<PageEntry>();

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
			+ key.len() + value_len(&entry.value)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorReadBufferUsage {
	pub node: FlowNodeId,
	pub resident: ByteSize,
	pub payload: ByteSize,
}

struct Shard {
	pages: HashMap<PageId, ResidentPage>,
	warming: HashMap<PageId, bool>,
	next_tick: u64,
	page_cap: usize,
	budget: MemoryBudget,
}

struct PoolInner {
	operator_shards: Box<[Mutex<Shard>]>,
	general_shards: Box<[Mutex<Shard>]>,
	bucket_shift: AtomicU8,
}

#[derive(Clone)]
pub struct MultiReadBufferTier {
	inner: Arc<PoolInner>,
}
