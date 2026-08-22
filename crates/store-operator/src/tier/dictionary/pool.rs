// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::Arc,
};

use reifydb_core::{
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

use crate::tier::dictionary::{
	DictionaryKey, EVICTION_SAMPLE, OperatorDictionaryConfig, OperatorDictionaryMetrics, OperatorDictionaryTier,
	PoolInner, Shard, entry_footprint,
};

const DICTIONARY_SCOPE: &str = "operator_dictionary";

impl OperatorDictionaryTier {
	pub fn new(config: OperatorDictionaryConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
			}),
		})
	}

	pub(super) fn shard_for(&self, key: &DictionaryKey) -> &Mutex<Shard> {
		let shards = &self.inner.shards;
		let mut hasher = DefaultHasher::new();
		key.suffix.hash(&mut hasher);
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

	pub fn entries(&self) -> usize {
		self.all_shards().map(|shard| shard.lock().slots.len()).sum()
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

	pub fn metrics(&self) -> OperatorDictionaryMetrics {
		let mut total = OperatorDictionaryMetrics::default();
		for shard in self.all_shards() {
			let shard = shard.lock();
			total.hits += shard.metrics.hits;
			total.misses += shard.metrics.misses;
			total.insertions += shard.metrics.insertions;
			total.evictions += shard.metrics.evictions;
			total.fills_started += shard.metrics.fills_started;
			total.fills_dirty_aborted += shard.metrics.fills_dirty_aborted;
			total.fills_duplicate += shard.metrics.fills_duplicate;
		}
		total
	}

	pub fn shard_limit_bytes(&self) -> ByteSize {
		self.inner.shards[0].lock().budget.limit()
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

impl MetricsCollector for OperatorDictionaryTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(DICTIONARY_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(DICTIONARY_SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::bytes(DICTIONARY_SCOPE, "shard_limit_bytes", self.shard_limit_bytes()));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "insertions", counters.insertions));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "fills_started", counters.fills_started));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "fills_dirty_aborted", counters.fills_dirty_aborted));
		out.push(MetricsSample::counter(DICTIONARY_SCOPE, "fills_duplicate", counters.fills_duplicate));
	}
}

fn build_shards(config: OperatorDictionaryConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|index| {
			Mutex::new(Shard {
				index: HashMap::new(),
				slots: Vec::new(),
				budget: MemoryBudget::new(byte_cap),
				filling: HashMap::new(),
				next_tick: 0,
				rng: 0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1),
				metrics: OperatorDictionaryMetrics::default(),
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

	fn pick_victim(&mut self) -> Option<usize> {
		let len = self.slots.len();
		if len == 0 {
			return None;
		}
		if len <= EVICTION_SAMPLE {
			return (0..len).min_by_key(|position| self.slots[*position].tick);
		}
		let mut victim: Option<(u64, usize)> = None;
		for _ in 0..EVICTION_SAMPLE {
			let position = (self.next_random() % len as u64) as usize;
			let tick = self.slots[position].tick;
			if victim.map(|(lowest, _)| tick < lowest).unwrap_or(true) {
				victim = Some((tick, position));
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

	pub(super) fn evict_to_capacity(&mut self) {
		while self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			self.remove_at(victim);
			self.metrics.evictions += 1;
		}
	}
}
