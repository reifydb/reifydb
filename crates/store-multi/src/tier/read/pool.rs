// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU8, Ordering},
	},
};

use reifydb_core::{
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::PageId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{
	MultiReadBufferTier, PoolInner, ReadBufferConfig, ReadBufferReadMetrics, ReadBufferShardMetrics,
	ReadBufferStateMetrics, ReadBufferWarmMetrics, Shard,
};

const READ_BUFFER_SCOPE: &str = "read_buffer";

impl MultiReadBufferTier {
	pub fn new(config: ReadBufferConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				bucket_shift: AtomicU8::new(config.bucket_shift),
				enabled: AtomicBool::new(true),
			}),
		})
	}

	pub(super) fn enabled(&self) -> bool {
		self.inner.enabled.load(Ordering::Relaxed)
	}

	pub fn set_budget(&self, budget: Option<ByteSize>) {
		let shards = &self.inner.shards;
		let byte_cap = budget
			.map(|bytes| ByteSize::from_bytes((bytes.as_bytes() / shards.len() as u64).max(1)))
			.unwrap_or(ByteSize::from_bytes(1));
		self.inner.enabled.store(budget.is_some(), Ordering::Relaxed);
		for shard in shards.iter() {
			let mut shard = shard.lock();
			shard.pages = HashMap::new();
			shard.warming = HashMap::new();
			shard.next_tick = 0;
			shard.budget = MemoryBudget::new(byte_cap);
		}
	}

	pub(super) fn bucket_shift(&self) -> u8 {
		self.inner.bucket_shift.load(Ordering::Relaxed)
	}

	pub(super) fn shard_for(&self, page: &PageId) -> &Mutex<Shard> {
		let shards = &self.inner.shards;
		let mut hasher = DefaultHasher::new();
		page.hash(&mut hasher);
		let index = (hasher.finish() % shards.len() as u64) as usize;
		&shards[index]
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.shards.iter()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		total_resident_bytes(&self.inner.shards)
	}

	pub fn resident_pages(&self) -> usize {
		total_resident_pages(&self.inner.shards)
	}

	pub fn payload_bytes(&self) -> ByteSize {
		total_payload_bytes(&self.inner.shards)
	}

	pub fn shard_metrics(&self) -> Vec<ReadBufferShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let mut payload = 0u64;
			let mut entries = 0usize;
			let mut hot_pages = 0usize;
			let mut complete_pages = 0usize;
			let mut blocked_pages = 0usize;
			for page in shard.pages.values() {
				payload += page.payload as u64;
				entries += page.entries.len();
				hot_pages += usize::from(page.hot);
				complete_pages += usize::from(page.range_complete);
				blocked_pages += usize::from(page.warm_blocked);
			}
			out.push(ReadBufferShardMetrics {
				shard: index,
				state: ReadBufferStateMetrics {
					used: shard.budget.used(),
					limit: shard.budget.limit(),
					pages: shard.pages.len(),
					page_cap: shard.page_cap,
					payload: ByteSize::from_bytes(payload),
					entries,
					hot_pages,
					complete_pages,
					blocked_pages,
					warming: shard.warming.len(),
				},
				warms: shard.warm_metrics,
				reads: shard.read_metrics,
			});
		}
		out
	}

	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.all_shards()
			.map(|shard| shard.lock().pages.values().map(|page| page.entries.len()).sum::<usize>())
			.sum()
	}

	#[cfg(test)]
	pub fn tallied_page_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.map(|shard| shard.lock().pages.values().map(|page| page.bytes as u64).sum::<u64>())
			.sum();
		ByteSize::from_bytes(total)
	}
}

impl MetricsCollector for MultiReadBufferTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap(READ_BUFFER_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::bytes(READ_BUFFER_SCOPE, "payload_bytes", self.payload_bytes()));
		out.push(MetricsSample::count(READ_BUFFER_SCOPE, "resident_pages", self.resident_pages() as u64));
		let scope = READ_BUFFER_SCOPE;
		let shards = &self.inner.shards;
		out.push(MetricsSample::bytes(scope, "shard_limit_bytes", shards[0].lock().budget.limit()));
		for (index, shard) in shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{scope}::shard::{index:02}"),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
	}
}

fn total_resident_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards.iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
	ByteSize::from_bytes(total)
}

fn total_payload_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards
		.iter()
		.map(|shard| shard.lock().pages.values().map(|page| page.payload as u64).sum::<u64>())
		.sum();
	ByteSize::from_bytes(total)
}

fn total_resident_pages(shards: &[Mutex<Shard>]) -> usize {
	shards.iter().map(|shard| shard.lock().pages.len()).sum()
}

fn build_shards(config: ReadBufferConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let page_cap = (config.resident_pages / shard_count).max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				pages: HashMap::new(),
				warming: HashMap::new(),
				next_tick: 0,
				page_cap,
				budget: MemoryBudget::new(byte_cap),
				warm_metrics: ReadBufferWarmMetrics::default(),
				read_metrics: ReadBufferReadMetrics::default(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
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
				self.warm_metrics.pages_evicted += 1;
			}
		}
	}
}
