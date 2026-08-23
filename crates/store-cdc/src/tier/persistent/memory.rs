// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{common::CommitVersion, error::diagnostic::internal::internal, event::metric::CdcEviction};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{Result, byte_size::ByteSize, count::Count, error, reifydb_assertions, value::datetime::DateTime};
use tracing::instrument;

use crate::{
	storage::{Cutoff, aggregate_evictions, merge_evictions, total_evicted_count},
	tier::persistent::CdcPersistentMetrics,
	types::{Block, BlockId, BlockSummary, DropOutcome},
};

struct Resident {
	block: Arc<Block>,
	rollup: Vec<CdcEviction>,
	stored_bytes: ByteSize,
}

struct MemoryCdcPersistentInner {
	blocks: Mutex<BTreeMap<CommitVersion, Resident>>,
	truncated_before: AtomicU64,
	stored_bytes: AtomicU64,
	appends: AtomicU64,
	loads: AtomicU64,
	drops: AtomicU64,
}

#[derive(Clone)]
pub struct MemoryCdcPersistent {
	inner: Arc<MemoryCdcPersistentInner>,
}

impl MemoryCdcPersistent {
	#[instrument(name = "store::cdc::persistent::memory::new", level = "debug")]
	pub fn new() -> Self {
		Self {
			inner: Arc::new(MemoryCdcPersistentInner {
				blocks: Mutex::new(BTreeMap::new()),
				truncated_before: AtomicU64::new(0),
				stored_bytes: AtomicU64::new(0),
				appends: AtomicU64::new(0),
				loads: AtomicU64::new(0),
				drops: AtomicU64::new(0),
			}),
		}
	}

	#[instrument(name = "store::cdc::persistent::memory::append_block", level = "debug", skip_all)]
	pub fn append_block(&self, block: &Block) -> Result<()> {
		let Some(first) = block.entries.first() else {
			return Err(error!(internal(
				"an empty cdc block has no version range and would break prefix truncation"
			)));
		};
		let last = block.entries.last().unwrap();
		reifydb_assertions! {
			assert!(
				block.entries.windows(2).all(|w| w[0].version < w[1].version),
				"block entries must be strictly ascending by version"
			);
			assert_eq!(
				block.summary.id.0, last.version,
				"a block is identified by its highest version"
			);
			assert_eq!(
				block.summary.min_version, first.version,
				"summary min_version must be the lowest entry version"
			);
			assert_eq!(
				block.summary.max_version, last.version,
				"summary max_version must be the highest entry version"
			);
			assert_eq!(
				block.summary.count.as_u64(), block.entries.len() as u64,
				"summary count must match the entries the payload carries"
			);
		}

		let stored_bytes = block.resident_bytes();
		let rollup = aggregate_evictions(block.entries.iter().flat_map(|entry| entry.changes.iter()));
		let summary = BlockSummary {
			id: BlockId(last.version),
			min_version: first.version,
			max_version: last.version,
			min_timestamp: block.entries.iter().map(|cdc| cdc.timestamp).min().unwrap(),
			max_timestamp: block.entries.iter().map(|cdc| cdc.timestamp).max().unwrap(),
			count: Count::new(block.entries.len() as u64),
			stored_bytes,
		};

		self.inner.blocks.lock().insert(
			last.version,
			Resident {
				block: Arc::new(Block {
					summary,
					entries: block.entries.clone(),
				}),
				rollup,
				stored_bytes,
			},
		);
		self.inner.stored_bytes.fetch_add(stored_bytes.as_bytes(), Ordering::Relaxed);
		self.inner.appends.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}

	#[instrument(name = "store::cdc::persistent::memory::load_block_containing", level = "trace", skip(self), fields(version = version.0))]
	pub fn load_block_containing(&self, version: CommitVersion) -> Result<Option<Arc<Block>>> {
		let guard = self.inner.blocks.lock();
		let hit = guard
			.range(version..)
			.next()
			.filter(|(_, resident)| resident.block.min_version() <= version)
			.map(|(_, resident)| Arc::clone(&resident.block));
		if hit.is_some() {
			self.inner.loads.fetch_add(1, Ordering::Relaxed);
		}
		Ok(hit)
	}

	#[instrument(name = "store::cdc::persistent::memory::load_block", level = "trace", skip(self), fields(block = id.0.0))]
	pub fn load_block(&self, id: BlockId) -> Result<Option<Arc<Block>>> {
		let guard = self.inner.blocks.lock();
		let hit = guard.get(&id.0).map(|resident| Arc::clone(&resident.block));
		if hit.is_some() {
			self.inner.loads.fetch_add(1, Ordering::Relaxed);
		}
		Ok(hit)
	}

	#[instrument(name = "store::cdc::persistent::memory::summaries_from", level = "trace", skip(self), fields(from = from.0, limit = limit))]
	pub fn summaries_from(&self, from: CommitVersion, limit: usize) -> Result<Vec<BlockSummary>> {
		let guard = self.inner.blocks.lock();
		Ok(guard.range(from..).take(limit).map(|(_, resident)| resident.block.summary).collect())
	}

	#[instrument(name = "store::cdc::persistent::memory::drop_blocks_below", level = "debug", skip(self), fields(cutoff = ?cutoff, limit = limit))]
	pub fn drop_blocks_below(&self, cutoff: Cutoff, limit: usize) -> Result<DropOutcome> {
		let mut guard = self.inner.blocks.lock();
		let scanned: Vec<CommitVersion> = match cutoff {
			Cutoff::Version(version) => {
				guard.range(..version).take(limit.saturating_add(1)).map(|(key, _)| *key).collect()
			}
			Cutoff::Unbounded => guard.keys().take(limit.saturating_add(1)).copied().collect(),
		};
		let more_remaining = scanned.len() > limit;
		let doomed = &scanned[..scanned.len().min(limit)];
		if doomed.is_empty() {
			return Ok(DropOutcome {
				count: Count::ZERO,
				entries: Vec::new(),
				more_remaining,
			});
		}

		let mut rollups: Vec<CdcEviction> = Vec::new();
		let mut freed = 0u64;
		let floor = doomed.iter().map(|key| key.0).max().unwrap_or(0).saturating_add(1);
		self.inner.truncated_before.fetch_max(floor, Ordering::Release);
		for key in doomed {
			let resident = guard.remove(key).unwrap();
			rollups.extend(resident.rollup);
			freed = freed.saturating_add(resident.stored_bytes.as_bytes());
		}
		drop(guard);

		let stored_bytes = self.inner.stored_bytes.load(Ordering::Relaxed);
		self.inner.stored_bytes.store(stored_bytes.saturating_sub(freed), Ordering::Relaxed);
		self.inner.drops.fetch_add(doomed.len() as u64, Ordering::Relaxed);

		let entries = merge_evictions(rollups);
		let count = total_evicted_count(&entries);
		Ok(DropOutcome {
			count,
			entries,
			more_remaining,
		})
	}

	#[instrument(name = "store::cdc::persistent::memory::min_version", level = "trace", skip(self))]
	pub fn min_version(&self) -> Result<Option<CommitVersion>> {
		Ok(self.inner.blocks.lock().values().next().map(|resident| resident.block.min_version()))
	}

	#[instrument(name = "store::cdc::persistent::memory::max_version", level = "trace", skip(self))]
	pub fn max_version(&self) -> Result<Option<CommitVersion>> {
		Ok(self.inner.blocks.lock().keys().next_back().copied())
	}

	#[instrument(name = "store::cdc::persistent::memory::find_ttl_cutoff", level = "debug", skip(self, cutoff))]
	pub fn find_ttl_cutoff(&self, cutoff: DateTime) -> Result<Option<Cutoff>> {
		let hit = {
			let guard = self.inner.blocks.lock();
			guard.values()
				.find(|resident| resident.block.summary.max_timestamp >= cutoff)
				.map(|resident| resident.block.min_version())
		};
		if let Some(version) = hit {
			return Ok(Some(Cutoff::Version(version)));
		}
		Ok(self.max_version()?.map(|_| Cutoff::Unbounded))
	}

	#[instrument(name = "store::cdc::persistent::memory::truncated_before", level = "trace", skip(self))]
	pub fn truncated_before(&self) -> CommitVersion {
		CommitVersion(self.inner.truncated_before.load(Ordering::Acquire))
	}

	#[instrument(name = "store::cdc::persistent::memory::metrics", level = "trace", skip(self))]
	pub fn metrics(&self) -> CdcPersistentMetrics {
		CdcPersistentMetrics {
			blocks: self.inner.blocks.lock().len() as u64,
			stored_bytes: ByteSize::from_bytes(self.inner.stored_bytes.load(Ordering::Relaxed)),
			appends: self.inner.appends.load(Ordering::Relaxed),
			loads: self.inner.loads.load(Ordering::Relaxed),
			drops: self.inner.drops.load(Ordering::Relaxed),
		}
	}

	pub fn shutdown(&self) {}
}

impl Default for MemoryCdcPersistent {
	fn default() -> Self {
		Self::new()
	}
}
