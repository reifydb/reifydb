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
use crate::tier::range::FillInterlock;
use crate::tier::range::{
	BucketId, KEYSPACE_SLOTS, OperatorRangeConfig, OperatorRangeKeyspaceMetrics, OperatorRangeMetrics,
	OperatorRangeShardMetrics, OperatorRangeTier, PoolInner, Shard,
};

const RANGE_SCOPE: &str = "operator_range";

impl OperatorRangeTier {
	pub fn new(config: OperatorRangeConfig) -> Option<Self> {
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
	pub(crate) fn with_interlock(config: OperatorRangeConfig, interlock: FillInterlock) -> Option<Self> {
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

	pub fn shard_limit_bytes(&self) -> ByteSize {
		self.inner.shards[0].lock().budget.limit()
	}

	pub fn metrics(&self) -> OperatorRangeMetrics {
		let mut total = OperatorRangeMetrics::default();
		for shard in self.all_shards() {
			accumulate(&mut total, &shard.lock().metrics);
		}
		total
	}

	pub fn shard_metrics(&self) -> Vec<OperatorRangeShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let entries = shard.buckets.values().map(|bucket| bucket.entries.len()).sum();
			out.push(OperatorRangeShardMetrics {
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

	pub fn keyspace_metrics(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		let mut used = vec![0u64; KEYSPACE_SLOTS];
		let mut buckets = vec![0usize; KEYSPACE_SLOTS];
		let mut entries = vec![0usize; KEYSPACE_SLOTS];
		let mut counters = vec![OperatorRangeMetrics::default(); KEYSPACE_SLOTS];

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

		let empty = OperatorRangeMetrics::default();
		(0..KEYSPACE_SLOTS)
			.filter(|slot| buckets[*slot] > 0 || counters[*slot] != empty)
			.map(|slot| OperatorRangeKeyspaceMetrics {
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

impl MetricsCollector for OperatorRangeTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(RANGE_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(RANGE_SCOPE, "resident_buckets", self.buckets() as u64));
		out.push(MetricsSample::count(RANGE_SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(RANGE_SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(RANGE_SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(RANGE_SCOPE, "fills", counters.fills));
		out.push(MetricsSample::counter(RANGE_SCOPE, "fills_declined", counters.fills_declined));
		out.push(MetricsSample::counter(RANGE_SCOPE, "fills_dirty_aborted", counters.fills_dirty_aborted));
		out.push(MetricsSample::counter(RANGE_SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(RANGE_SCOPE, "point_hits", counters.point_hits));
		out.push(MetricsSample::counter(RANGE_SCOPE, "point_misses", counters.point_misses));
		out.push(MetricsSample::bytes(RANGE_SCOPE, "shard_limit_bytes", self.shard_limit_bytes()));
		for (index, shard) in self.inner.shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{RANGE_SCOPE}::shard::{index:02}"),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
		for keyspace in self.keyspace_metrics() {
			let scope = format!("{RANGE_SCOPE}::keyspace::{}", keyspace.keyspace.name());
			out.push(MetricsSample::bytes(scope.clone(), "used_bytes", keyspace.used));
			out.push(MetricsSample::count(scope.clone(), "buckets", keyspace.buckets as u64));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope.clone(), "misses", keyspace.counters.misses));
			out.push(MetricsSample::counter(scope.clone(), "evictions", keyspace.counters.evictions));
			out.push(MetricsSample::counter(scope.clone(), "point_hits", keyspace.counters.point_hits));
			out.push(MetricsSample::counter(scope, "point_misses", keyspace.counters.point_misses));
		}
	}
}

fn accumulate(target: &mut OperatorRangeMetrics, source: &OperatorRangeMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.fills += source.fills;
	target.fills_declined += source.fills_declined;
	target.fills_dirty_aborted += source.fills_dirty_aborted;
	target.evictions += source.evictions;
	target.point_hits += source.point_hits;
	target.point_misses += source.point_misses;
}

fn build_shards(config: OperatorRangeConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				buckets: HashMap::new(),
				filling: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				metrics: OperatorRangeMetrics::default(),
				keyspace_metrics: Box::new([OperatorRangeMetrics::default(); KEYSPACE_SLOTS]),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

impl Shard {
	fn pick_victim(&self) -> Option<BucketId> {
		let mut victim: Option<(u64, BucketId)> = None;
		for (id, bucket) in self.buckets.iter() {
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
