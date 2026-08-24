// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	array,
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	key::operator_state::Keyspace,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::point::FillInterlock;
use crate::tier::point::{
	EVICTION_SAMPLE, KEYSPACE_SLOTS, OperatorPointConfig, OperatorPointKeyspaceMetrics, OperatorPointMetrics,
	OperatorPointShardMetrics, OperatorPointTier, PointKey, PoolInner, Shard, Slot, entry_footprint, keyspace_of,
	sketch::Sketch,
};

const POINT_SCOPE: &str = "operator_point";

impl OperatorPointTier {
	pub fn new(config: OperatorPointConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				excluded_misses: array::from_fn(|_| AtomicU64::new(0)),
				#[cfg(test)]
				interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_interlock(config: OperatorPointConfig, interlock: FillInterlock) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				excluded_misses: array::from_fn(|_| AtomicU64::new(0)),
				interlock: Some(interlock),
			}),
		})
	}

	pub(super) fn shard_for(&self, key: &PointKey) -> &Mutex<Shard> {
		let shards = &self.inner.shards;
		let mut hasher = DefaultHasher::new();
		key.operator.hash(&mut hasher);
		key.key.as_slice().hash(&mut hasher);
		let index = (hasher.finish() % shards.len() as u64) as usize;
		&shards[index]
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.shards.iter()
	}

	pub(super) fn charge_excluded_miss(&self, keyspace: Keyspace) {
		self.inner.excluded_misses[keyspace.0 as usize].fetch_add(1, Ordering::Relaxed);
	}

	fn excluded_misses(&self, keyspace: usize) -> u64 {
		self.inner.excluded_misses[keyspace].load(Ordering::Relaxed)
	}

	fn excluded_misses_total(&self) -> u64 {
		self.inner.excluded_misses.iter().map(|slot| slot.load(Ordering::Relaxed)).sum()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	pub fn entries(&self) -> usize {
		self.all_shards().map(|shard| shard.lock().slots.len()).sum()
	}

	pub fn hits(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().metrics.hits).sum()
	}

	pub fn misses(&self) -> u64 {
		let sharded: u64 = self.all_shards().map(|shard| shard.lock().metrics.misses).sum();
		sharded + self.excluded_misses_total()
	}

	pub fn evictions(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().metrics.evictions).sum()
	}

	pub fn metrics(&self) -> OperatorPointMetrics {
		let mut total = OperatorPointMetrics::default();
		for shard in self.all_shards() {
			let shard = shard.lock();
			accumulate(&mut total, &shard.metrics);
		}
		total.misses += self.excluded_misses_total();
		total
	}

	pub fn shard_metrics(&self) -> Vec<OperatorPointShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			out.push(OperatorPointShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				entries: shard.slots.len(),
				counters: shard.metrics,
			});
		}
		out
	}

	pub fn keyspace_metrics(&self) -> Vec<OperatorPointKeyspaceMetrics> {
		let mut used = vec![0u64; KEYSPACE_SLOTS];
		let mut entries = vec![0usize; KEYSPACE_SLOTS];
		let mut counters = vec![OperatorPointMetrics::default(); KEYSPACE_SLOTS];

		for shard in self.all_shards() {
			let shard = shard.lock();
			for slot in &shard.slots {
				let keyspace = slot_keyspace(slot).0 as usize;
				used[keyspace] += entry_footprint(&slot.key, &slot.row) as u64;
				entries[keyspace] += 1;
			}
			for (keyspace, source) in shard.keyspace_metrics.iter().enumerate() {
				accumulate(&mut counters[keyspace], source);
			}
		}
		for (keyspace, counter) in counters.iter_mut().enumerate() {
			counter.misses += self.excluded_misses(keyspace);
		}

		let empty = OperatorPointMetrics::default();
		(0..KEYSPACE_SLOTS)
			.filter(|keyspace| entries[*keyspace] > 0 || counters[*keyspace] != empty)
			.map(|keyspace| OperatorPointKeyspaceMetrics {
				keyspace: Keyspace(keyspace as u8),
				used: ByteSize::from_bytes(used[keyspace]),
				entries: entries[keyspace],
				counters: counters[keyspace],
			})
			.collect()
	}

	pub fn shard_limit_bytes(&self) -> ByteSize {
		self.inner.shards[0].lock().budget.limit()
	}

	pub fn sketch_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().sketch.bytes() as u64).sum();
		ByteSize::from_bytes(total)
	}

	pub fn sketch_resets(&self) -> u64 {
		self.all_shards().map(|shard| shard.lock().sketch.resets()).sum()
	}

	#[cfg(test)]
	pub(crate) fn tallied_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.map(|shard| {
				shard.lock()
					.slots
					.iter()
					.map(|slot| entry_footprint(&slot.key, &slot.row) as u64)
					.sum::<u64>()
			})
			.sum();
		ByteSize::from_bytes(total)
	}

	#[cfg(test)]
	pub(crate) fn occupied_shards(&self) -> usize {
		self.all_shards().filter(|shard| !shard.lock().slots.is_empty()).count()
	}

	#[cfg(test)]
	pub(crate) fn index_is_consistent(&self) -> bool {
		self.all_shards().all(|shard| {
			let shard = shard.lock();
			shard.index.len() == shard.slots.len()
				&& shard.index.iter().all(|(key, position)| {
					shard.slots.get(*position).is_some_and(|slot| slot.key == *key)
				})
		})
	}
}

impl MetricsCollector for OperatorPointTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(POINT_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(POINT_SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(POINT_SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(POINT_SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(POINT_SCOPE, "insertions", counters.insertions));
		out.push(MetricsSample::counter(POINT_SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(POINT_SCOPE, "fills_started", counters.fills_started));
		out.push(MetricsSample::counter(POINT_SCOPE, "fills_dirty_aborted", counters.fills_dirty_aborted));
		out.push(MetricsSample::counter(POINT_SCOPE, "fills_duplicate", counters.fills_duplicate));
		out.push(MetricsSample::counter(POINT_SCOPE, "admissions_refused", counters.admissions_refused));
		out.push(MetricsSample::bytes(POINT_SCOPE, "shard_limit_bytes", self.shard_limit_bytes()));
		out.push(MetricsSample::bytes(POINT_SCOPE, "sketch_bytes", self.sketch_bytes()));
		out.push(MetricsSample::counter(POINT_SCOPE, "sketch_resets", self.sketch_resets()));
		for (index, shard) in self.inner.shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{POINT_SCOPE}::shard::{index:02}"),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
		for keyspace in self.keyspace_metrics() {
			let scope = format!("{POINT_SCOPE}::keyspace::{}", keyspace.keyspace.name());
			out.push(MetricsSample::bytes(scope.clone(), "used_bytes", keyspace.used));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope.clone(), "misses", keyspace.counters.misses));
			out.push(MetricsSample::counter(scope.clone(), "evictions", keyspace.counters.evictions));
			out.push(MetricsSample::counter(
				scope,
				"admissions_refused",
				keyspace.counters.admissions_refused,
			));
		}
	}
}

fn accumulate(target: &mut OperatorPointMetrics, source: &OperatorPointMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.insertions += source.insertions;
	target.evictions += source.evictions;
	target.fills_started += source.fills_started;
	target.fills_dirty_aborted += source.fills_dirty_aborted;
	target.fills_duplicate += source.fills_duplicate;
	target.admissions_refused += source.admissions_refused;
}

fn slot_keyspace(slot: &Slot) -> Keyspace {
	keyspace_of(&slot.key.key).expect("a resident slot carries a keyspace, or it was admitted past the guard")
}

fn build_shards(config: OperatorPointConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|index| {
			Mutex::new(Shard {
				index: HashMap::new(),
				slots: Vec::new(),
				filling: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				rng: 0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1),
				sketch: Sketch::new(config.sketch_counters),
				metrics: OperatorPointMetrics::default(),
				keyspace_metrics: Box::new([OperatorPointMetrics::default(); KEYSPACE_SLOTS]),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

impl Shard {
	fn next_random(&mut self) -> u64 {
		let mut state = self.rng;
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		self.rng = state;
		state
	}

	fn rank(&self, position: usize) -> (u8, u64) {
		let slot = &self.slots[position];
		(self.sketch.estimate(&slot.key), slot.tick)
	}

	fn pick_victim(&mut self) -> Option<usize> {
		let len = self.slots.len();
		if len == 0 {
			return None;
		}
		if len <= EVICTION_SAMPLE {
			return (0..len).min_by_key(|position| self.rank(*position));
		}
		let mut victim: Option<((u8, u64), usize)> = None;
		for _ in 0..EVICTION_SAMPLE {
			let position = (self.next_random() % len as u64) as usize;
			let rank = self.rank(position);
			if victim.map(|(lowest, _)| rank < lowest).unwrap_or(true) {
				victim = Some((rank, position));
			}
		}
		victim.map(|(_, position)| position)
	}

	pub(super) fn remove_at(&mut self, position: usize) {
		let slot = self.slots.swap_remove(position);
		self.index.remove(&slot.key);
		self.budget.release(ByteSize::from_bytes(entry_footprint(&slot.key, &slot.row) as u64));
		if position < self.slots.len() {
			let moved = self.slots[position].key.clone();
			self.index.insert(moved, position);
		}
	}

	pub(super) fn admits(&mut self, candidate: &PointKey) -> bool {
		let Some(victim) = self.pick_victim() else {
			return true;
		};
		let incumbent = self.sketch.estimate(&self.slots[victim].key);
		self.sketch.estimate(candidate) >= incumbent
	}

	pub(super) fn record_access(&mut self, key: &PointKey) {
		let population = self.slots.len();
		self.sketch.record(key, population);
	}

	pub(super) fn evict_to_capacity(&mut self) {
		while self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			let keyspace = slot_keyspace(&self.slots[victim]);
			self.remove_at(victim);
			self.metrics.evictions += 1;
			self.keyspace_metrics[keyspace.0 as usize].evictions += 1;
		}
	}
}
