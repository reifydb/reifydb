// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the multi-version store. Serves cold keys that the commit buffer has already evicted below
//! the eviction watermark, so a repeated point read does not have to fall through to the persistent tier every
//! time. Only the latest committed `(version, value)` per key is cached; a hit is served only when the requested
//! snapshot version is at or above the cached version, otherwise the caller reads through to the persistent tier
//! which honors the full version bound. Range scans consult this tier only for buckets marked `range_complete`: a
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
	sync::{Arc, atomic::AtomicU8},
};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
use reifydb_value::util::cowvec::CowVec;

use crate::tier::RangeBatch;

#[derive(Clone, Copy, Debug)]
pub struct ReadBufferConfig {
	pub resident_pages: usize,
	pub bucket_shift: u8,
	pub shards: usize,
}

impl Default for ReadBufferConfig {
	fn default() -> Self {
		Self {
			resident_pages: 1024,
			bucket_shift: DEFAULT_BUCKET_SHIFT,
			shards: 16,
		}
	}
}

#[derive(Clone)]
struct PageEntry {
	version: CommitVersion,
	value: Option<CowVec<u8>>,
}

struct ResidentPage {
	entries: BTreeMap<EncodedKey, PageEntry>,
	hot: bool,
	tick: u64,
	range_complete: bool,
}

pub enum ServedChunk {
	Served(RangeBatch),
	Gap,
}

struct Shard {
	pages: HashMap<PageId, ResidentPage>,
	next_tick: u64,
	page_cap: usize,
}

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	bucket_shift: AtomicU8,
}

#[derive(Clone)]
pub struct MultiReadBufferTier {
	inner: Arc<PoolInner>,
}
