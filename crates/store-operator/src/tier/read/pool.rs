// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::Arc,
};

use reifydb_core::util::budget::MemoryBudget;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{
	BucketId, OperatorReadBufferConfig, OperatorReadBufferMetrics, OperatorReadBufferShardMetrics,
	OperatorReadBufferTier, PoolInner, Shard,
};

impl OperatorReadBufferTier {
	pub fn new(config: OperatorReadBufferConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
			}),
		})
	}

	pub(super) fn shard_for(&self, bucket: &BucketId) -> &Mutex<Shard> {
		let shards = &self.inner.shards;
		let mut hasher = DefaultHasher::new();
		bucket.hash(&mut hasher);
		let index = (hasher.finish() % shards.len() as u64) as usize;
		&shards[index]
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.shards.iter()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	pub fn buckets(&self) -> usize {
		self.all_shards().map(|shard| shard.lock().buckets.len()).sum()
	}

	pub fn entries(&self) -> usize {
		self.all_shards()
			.map(|shard| shard.lock().buckets.values().map(|bucket| bucket.entries.len()).sum::<usize>())
			.sum()
	}

	pub fn complete_buckets(&self) -> usize {
		self.all_shards()
			.map(|shard| shard.lock().buckets.values().filter(|bucket| bucket.complete).count())
			.sum()
	}

	pub fn hits(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().metrics.hits).sum()
	}

	pub fn misses(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().metrics.misses).sum()
	}

	pub fn evictions(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().metrics.evictions).sum()
	}

	pub fn metrics(&self) -> OperatorReadBufferMetrics {
		let mut total = OperatorReadBufferMetrics::default();
		for shard in self.all_shards() {
			let shard = shard.lock();
			total.hits += shard.metrics.hits;
			total.misses += shard.metrics.misses;
			total.evictions += shard.metrics.evictions;
		}
		total
	}

	pub fn shard_metrics(&self) -> Vec<OperatorReadBufferShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let mut entries = 0usize;
			let mut complete_buckets = 0usize;
			for bucket in shard.buckets.values() {
				entries += bucket.entries.len();
				complete_buckets += usize::from(bucket.complete);
			}
			out.push(OperatorReadBufferShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				buckets: shard.buckets.len(),
				entries,
				complete_buckets,
				counters: shard.metrics,
			});
		}
		out
	}

	#[cfg(test)]
	pub fn tallied_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.map(|shard| shard.lock().buckets.values().map(|bucket| bucket.bytes as u64).sum::<u64>())
			.sum();
		ByteSize::from_bytes(total)
	}
}

fn build_shards(config: OperatorReadBufferConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				buckets: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				metrics: OperatorReadBufferMetrics::default(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

impl Shard {
	fn pick_victim(&self) -> Option<BucketId> {
		let mut victim: Option<(u64, BucketId)> = None;
		for (id, bucket) in &self.buckets {
			if victim.map(|(tick, _)| bucket.tick < tick).unwrap_or(true) {
				victim = Some((bucket.tick, *id));
			}
		}
		victim.map(|(_, id)| id)
	}

	pub(super) fn evict_to_capacity(&mut self) {
		while self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			let Some(bucket) = self.buckets.remove(&victim) else {
				break;
			};
			self.budget.release(ByteSize::from_bytes(bucket.bytes as u64));
			self.metrics.evictions += 1;
		}
	}
}
