// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering},
	},
};

use reifydb_core::util::budget::MemoryBudget;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::PageId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{MultiReadBufferTier, PoolInner, ReadBufferConfig, Shard};

impl MultiReadBufferTier {
	pub fn new(config: ReadBufferConfig) -> Self {
		let shard_count = config.shards.max(1);
		let page_cap = (config.resident_pages / shard_count).max(1);
		let byte_cap = ByteSize::from_bytes((config.resident_bytes.as_bytes() / shard_count as u64).max(1));
		let shards: Vec<Mutex<Shard>> = (0..shard_count)
			.map(|_| {
				Mutex::new(Shard {
					pages: HashMap::new(),
					warming: HashMap::new(),
					next_tick: 0,
					page_cap,
					budget: MemoryBudget::new(byte_cap),
				})
			})
			.collect();
		Self {
			inner: Arc::new(PoolInner {
				shards: shards.into_boxed_slice(),
				bucket_shift: AtomicU8::new(config.bucket_shift),
			}),
		}
	}

	pub(super) fn bucket_shift(&self) -> u8 {
		self.inner.bucket_shift.load(Ordering::Relaxed)
	}

	pub(super) fn shard_for(&self, page: &PageId) -> &Mutex<Shard> {
		let mut hasher = DefaultHasher::new();
		page.hash(&mut hasher);
		let index = (hasher.finish() % self.inner.shards.len() as u64) as usize;
		&self.inner.shards[index]
	}

	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.inner
			.shards
			.iter()
			.map(|shard| shard.lock().pages.values().map(|page| page.entries.len()).sum::<usize>())
			.sum()
	}

	#[cfg(test)]
	pub fn resident_pages(&self) -> usize {
		self.inner.shards.iter().map(|shard| shard.lock().pages.len()).sum()
	}

	#[cfg(test)]
	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.inner.shards.iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	#[cfg(test)]
	pub fn tallied_page_bytes(&self) -> ByteSize {
		let total = self
			.inner
			.shards
			.iter()
			.map(|shard| shard.lock().pages.values().map(|page| page.bytes as u64).sum::<u64>())
			.sum();
		ByteSize::from_bytes(total)
	}
}

impl Shard {
	fn pick_victim(&self) -> Option<PageId> {
		let mut probationary: Option<(u64, PageId)> = None;
		let mut hot: Option<(u64, PageId)> = None;
		for (id, page) in &self.pages {
			let slot = if page.hot {
				&mut hot
			} else {
				&mut probationary
			};
			if slot.map(|(tick, _)| page.tick < tick).unwrap_or(true) {
				*slot = Some((page.tick, *id));
			}
		}
		probationary.or(hot).map(|(_, id)| id)
	}

	pub(super) fn evict_to_capacity(&mut self) {
		while self.pages.len() > self.page_cap || self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			if let Some(page) = self.pages.remove(&victim) {
				self.budget.release(ByteSize::from_bytes(page.bytes as u64));
			}
		}
	}
}
