// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use hashbrown::HashTable;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::point::FillInterlock;
#[cfg(test)]
use crate::tier::point::PointKey;
use crate::tier::point::{
	EVICTION_SAMPLE, Entry, PointConfig, PointDomain, PointMetrics, PointShardMetrics, PointSlotMetrics, PointTier,
	PoolInner, Shard, entry_footprint, entry_hash,
};

impl<D: PointDomain> PointTier<D> {
	pub fn new(config: PointConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards::<D>(config, resident_bytes),
				excluded_misses: build_excluded_misses::<D>(),
				#[cfg(test)]
				interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_interlock(config: PointConfig, interlock: FillInterlock<D>) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards::<D>(config, resident_bytes),
				excluded_misses: build_excluded_misses::<D>(),
				interlock: Some(interlock),
			}),
		})
	}

	pub fn shard_index(&self, dimension: D::Dimension, key: &EncodedKey) -> usize {
		let mut hasher = DefaultHasher::new();
		dimension.hash(&mut hasher);
		key.as_slice().hash(&mut hasher);
		(hasher.finish() % self.inner.shards.len() as u64) as usize
	}

	pub(super) fn shard_at(&self, dimension: D::Dimension, key: &EncodedKey) -> &Mutex<Shard<D>> {
		&self.inner.shards[self.shard_index(dimension, key)]
	}

	#[cfg(test)]
	pub(super) fn shard_for(&self, key: &PointKey<D::Dimension>) -> &Mutex<Shard<D>> {
		self.shard_at(key.dimension, &key.key)
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard<D>>> {
		self.inner.shards.iter()
	}

	pub(super) fn charge_excluded_miss(&self, slot: usize) {
		self.inner.excluded_misses[slot].fetch_add(1, Ordering::Relaxed);
	}

	fn excluded_misses(&self, slot: usize) -> u64 {
		self.inner.excluded_misses[slot].load(Ordering::Relaxed)
	}

	fn excluded_misses_total(&self) -> u64 {
		self.inner.excluded_misses.iter().map(|slot| slot.load(Ordering::Relaxed)).sum()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	pub fn entries(&self) -> usize {
		self.all_shards().map(|shard| shard.lock().entries.len()).sum()
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

	pub fn metrics(&self) -> PointMetrics {
		let mut total = PointMetrics::default();
		for shard in self.all_shards() {
			let shard = shard.lock();
			accumulate(&mut total, &shard.metrics);
		}
		total.misses += self.excluded_misses_total();
		total
	}

	pub fn shard_metrics(&self) -> Vec<PointShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			out.push(PointShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				entries: shard.entries.len(),
				counters: shard.metrics,
			});
		}
		out
	}

	pub fn slot_metrics(&self) -> Vec<PointSlotMetrics<D>> {
		let mut used = vec![0u64; D::SLOTS];
		let mut entries = vec![0usize; D::SLOTS];
		let mut counters = vec![PointMetrics::default(); D::SLOTS];

		for shard in self.all_shards() {
			let shard = shard.lock();
			for entry in &shard.entries {
				let slot = entry_slot::<D>(entry);
				used[slot] += entry_footprint::<D>(&entry.key, &entry.row) as u64;
				entries[slot] += 1;
			}
			for (slot, source) in shard.slot_metrics.iter().enumerate() {
				accumulate(&mut counters[slot], source);
			}
		}
		for (slot, counter) in counters.iter_mut().enumerate() {
			counter.misses += self.excluded_misses(slot);
		}

		let empty = PointMetrics::default();
		(0..D::SLOTS)
			.filter(|slot| entries[*slot] > 0 || counters[*slot] != empty)
			.map(|slot| PointSlotMetrics {
				slot: D::slot_at(slot),
				used: ByteSize::from_bytes(used[slot]),
				entries: entries[slot],
				counters: counters[slot],
			})
			.collect()
	}

	pub fn shard_limit_bytes(&self) -> ByteSize {
		self.inner.shards[0].lock().budget.limit()
	}

	#[cfg(test)]
	pub(crate) fn tallied_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.map(|shard| {
				shard.lock()
					.entries
					.iter()
					.map(|entry| entry_footprint::<D>(&entry.key, &entry.row) as u64)
					.sum::<u64>()
			})
			.sum();
		ByteSize::from_bytes(total)
	}

	#[cfg(test)]
	pub(crate) fn occupied_shards(&self) -> usize {
		self.all_shards().filter(|shard| !shard.lock().entries.is_empty()).count()
	}

	#[cfg(test)]
	pub(crate) fn index_is_consistent(&self) -> bool {
		self.all_shards().all(|shard| {
			let shard = shard.lock();
			let Shard {
				index,
				entries,
				..
			} = &*shard;
			index.len() == entries.len()
				&& entries.iter().enumerate().all(|(position, entry)| {
					index.find(entry_hash::<D>(entry), |resident| {
						let other = &entries[*resident as usize];
						other.key.dimension == entry.key.dimension
							&& other.key.key == entry.key.key
					})
					.copied() == Some(position as u32)
				})
		})
	}
}

impl<D: PointDomain> MetricsCollector for PointTier<D> {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(D::SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(D::SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(D::SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(D::SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(D::SCOPE, "insertions", counters.insertions));
		out.push(MetricsSample::counter(D::SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(D::SCOPE, "fills_started", counters.fills_started));
		out.push(MetricsSample::counter(D::SCOPE, "fills_dirty_aborted", counters.fills_dirty_aborted));
		out.push(MetricsSample::counter(D::SCOPE, "fills_duplicate", counters.fills_duplicate));
		out.push(MetricsSample::bytes(D::SCOPE, "shard_limit_bytes", self.shard_limit_bytes()));
		for (index, shard) in self.inner.shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{}::shard::{index:02}", D::SCOPE),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
		for keyspace in self.slot_metrics() {
			let scope = format!("{}::keyspace::{}", D::SCOPE, D::slot_name(keyspace.slot));
			out.push(MetricsSample::bytes(scope.clone(), "used_bytes", keyspace.used));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope.clone(), "misses", keyspace.counters.misses));
			out.push(MetricsSample::counter(scope, "evictions", keyspace.counters.evictions));
		}
	}
}

fn accumulate(target: &mut PointMetrics, source: &PointMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.insertions += source.insertions;
	target.evictions += source.evictions;
	target.fills_started += source.fills_started;
	target.fills_dirty_aborted += source.fills_dirty_aborted;
	target.fills_duplicate += source.fills_duplicate;
}

fn entry_slot<D: PointDomain>(entry: &Entry<D>) -> usize {
	D::slot(&entry.key.key).expect("a resident entry carries a slot, or it was admitted past the guard")
}

fn build_excluded_misses<D: PointDomain>() -> Box<[AtomicU64]> {
	(0..D::SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice()
}

fn build_shards<D: PointDomain>(config: PointConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard<D>>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|index| {
			Mutex::new(Shard {
				index: HashTable::new(),
				entries: Vec::new(),
				filling: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				rng: 0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1),
				metrics: PointMetrics::default(),
				slot_metrics: vec![PointMetrics::default(); D::SLOTS].into_boxed_slice(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

impl<D: PointDomain> Shard<D> {
	fn next_random(&mut self) -> u64 {
		let mut state = self.rng;
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		self.rng = state;
		state
	}

	fn pick_victim(&mut self) -> Option<usize> {
		let len = self.entries.len();
		if len == 0 {
			return None;
		}
		if len <= EVICTION_SAMPLE {
			return (0..len).min_by_key(|position| self.entries[*position].tick);
		}
		let mut victim: Option<(u64, usize)> = None;
		for _ in 0..EVICTION_SAMPLE {
			let position = (self.next_random() % len as u64) as usize;
			let tick = self.entries[position].tick;
			if victim.map(|(lowest, _)| tick < lowest).unwrap_or(true) {
				victim = Some((tick, position));
			}
		}
		victim.map(|(_, position)| position)
	}

	pub(super) fn remove_at(&mut self, position: usize) {
		let Shard {
			index,
			entries,
			budget,
			..
		} = self;
		let last = entries.len() - 1;
		let entry = entries.swap_remove(position);
		if let Ok(occupied) =
			index.find_entry(entry_hash::<D>(&entry), |resident| *resident as usize == position)
		{
			occupied.remove();
		}
		if position < entries.len() {
			let moved = entry_hash::<D>(&entries[position]);
			if let Some(resident) = index.find_mut(moved, |resident| *resident as usize == last) {
				*resident = position as u32;
			}
		}
		budget.release(ByteSize::from_bytes(entry_footprint::<D>(&entry.key, &entry.row) as u64));
	}

	pub(super) fn evict_to_capacity(&mut self) {
		while self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			let slot = entry_slot::<D>(&self.entries[victim]);
			self.remove_at(victim);
			self.metrics.evictions += 1;
			self.slot_metrics[slot].evictions += 1;
		}
	}
}
