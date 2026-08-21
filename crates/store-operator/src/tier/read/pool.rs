// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::Arc,
};

use reifydb_core::{
	key::operator_state::Keyspace,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::read::FillInterlock;
use crate::tier::read::{
	BucketId, KEYSPACE_SLOTS, OperatorReadBufferConfig, OperatorReadBufferKeyspaceMetrics,
	OperatorReadBufferMetrics, OperatorReadBufferShardMetrics, OperatorReadBufferTier, PoolInner, Shard,
};

const READ_BUFFER_SCOPE: &str = "operator_read_buffer";

impl OperatorReadBufferTier {
	pub fn new(config: OperatorReadBufferConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				#[cfg(test)]
				interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_interlock(config: OperatorReadBufferConfig, interlock: FillInterlock) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				interlock: Some(interlock),
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
			total.fills_started += shard.metrics.fills_started;
			total.fills_dirty_aborted += shard.metrics.fills_dirty_aborted;
			total.fills_duplicate += shard.metrics.fills_duplicate;
		}
		total
	}

	pub fn shard_metrics(&self) -> Vec<OperatorReadBufferShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let mut entries = 0usize;
			for bucket in shard.buckets.values() {
				entries += bucket.entries.len();
			}
			out.push(OperatorReadBufferShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				buckets: shard.buckets.len(),
				entries,
				counters: shard.metrics,
			});
		}
		out
	}

	pub fn keyspace_metrics(&self) -> Vec<OperatorReadBufferKeyspaceMetrics> {
		let mut used = vec![0u64; KEYSPACE_SLOTS];
		let mut buckets = vec![0usize; KEYSPACE_SLOTS];
		let mut entries = vec![0usize; KEYSPACE_SLOTS];
		let mut counters = vec![OperatorReadBufferMetrics::default(); KEYSPACE_SLOTS];

		for shard in self.all_shards() {
			let shard = shard.lock();
			for (id, bucket) in &shard.buckets {
				let slot = id.keyspace.0 as usize;
				used[slot] += bucket.bytes as u64;
				buckets[slot] += 1;
				entries[slot] += bucket.entries.len();
			}
			for (slot, source) in shard.keyspace_metrics.iter().enumerate() {
				accumulate(&mut counters[slot], source);
			}
		}

		let empty = OperatorReadBufferMetrics::default();
		(0..KEYSPACE_SLOTS)
			.filter(|slot| buckets[*slot] > 0 || counters[*slot] != empty)
			.map(|slot| OperatorReadBufferKeyspaceMetrics {
				keyspace: Keyspace(slot as u8),
				used: ByteSize::from_bytes(used[slot]),
				buckets: buckets[slot],
				entries: entries[slot],
				counters: counters[slot],
			})
			.collect()
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

impl MetricsCollector for OperatorReadBufferTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(READ_BUFFER_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(READ_BUFFER_SCOPE, "resident_buckets", self.buckets() as u64));
		out.push(MetricsSample::count(READ_BUFFER_SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(READ_BUFFER_SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(READ_BUFFER_SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(READ_BUFFER_SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(READ_BUFFER_SCOPE, "fills_started", counters.fills_started));
		out.push(MetricsSample::counter(
			READ_BUFFER_SCOPE,
			"fills_dirty_aborted",
			counters.fills_dirty_aborted,
		));
		out.push(MetricsSample::counter(READ_BUFFER_SCOPE, "fills_duplicate", counters.fills_duplicate));
		let shards = &self.inner.shards;
		out.push(MetricsSample::bytes(READ_BUFFER_SCOPE, "shard_limit_bytes", shards[0].lock().budget.limit()));
		for (index, shard) in shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{READ_BUFFER_SCOPE}::shard::{index:02}"),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
	}
}

fn accumulate(target: &mut OperatorReadBufferMetrics, source: &OperatorReadBufferMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.evictions += source.evictions;
	target.fills_started += source.fills_started;
	target.fills_dirty_aborted += source.fills_dirty_aborted;
	target.fills_duplicate += source.fills_duplicate;
}

fn build_shards(config: OperatorReadBufferConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				buckets: HashMap::new(),
				filling: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				metrics: OperatorReadBufferMetrics::default(),
				keyspace_metrics: Box::new([OperatorReadBufferMetrics::default(); KEYSPACE_SLOTS]),
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
			self.keyspace_metrics[victim.keyspace.0 as usize].evictions += 1;
		}
	}
}
