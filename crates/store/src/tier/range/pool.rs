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
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::range::{MaterializeInterlock, ServeInterlock};
use crate::{
	coverage::{index::CoverageIndex, plan::GapHistogram, retraction::Retractions},
	tier::range::{
		Partition, PoolInner, Progress, RangeConfig, RangeDomain, RangeMetrics, RangeShardMetrics,
		RangeSlotMetrics, RangeTier, Shard, account, entry_footprint,
	},
};

const GAP_SLOTS: [&str; 8] =
	["count_0", "count_1", "count_2", "count_3", "count_4", "count_5_8", "count_9_16", "count_17_plus"];

impl<D: RangeDomain> RangeTier<D> {
	pub fn new(config: RangeConfig) -> Option<Self> {
		let shard_bytes = config.shard_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards::<D>(config, shard_bytes),
				coverage: RwLock::new(CoverageIndex::new()),
				retractions: Retractions::new(),
				gap_guard: config.gap_guard,
				#[cfg(test)]
				interlock: None,
				#[cfg(test)]
				serve_interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_interlock(config: RangeConfig, interlock: MaterializeInterlock<D>) -> Option<Self> {
		let shard_bytes = config.shard_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards::<D>(config, shard_bytes),
				coverage: RwLock::new(CoverageIndex::new()),
				retractions: Retractions::new(),
				gap_guard: config.gap_guard,
				interlock: Some(interlock),
				serve_interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_serve_interlock(config: RangeConfig, interlock: ServeInterlock<D>) -> Option<Self> {
		let shard_bytes = config.shard_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards::<D>(config, shard_bytes),
				coverage: RwLock::new(CoverageIndex::new()),
				retractions: Retractions::new(),
				gap_guard: config.gap_guard,
				interlock: None,
				serve_interlock: Some(interlock),
			}),
		})
	}

	#[cfg(test)]
	pub(super) fn fire_serve_interlock(&self) {
		if let Some(interlock) = self.inner.serve_interlock.as_ref() {
			interlock(self);
		}
	}

	pub fn shard_index(&self, partition: &D::Partition) -> usize {
		let mut hasher = DefaultHasher::new();
		partition.hash(&mut hasher);
		(hasher.finish() % self.inner.shards.len() as u64) as usize
	}

	pub(super) fn shard(&self, index: usize) -> &Mutex<Shard<D>> {
		&self.inner.shards[index]
	}

	pub(super) fn shard_for(&self, partition: &D::Partition) -> &Mutex<Shard<D>> {
		&self.inner.shards[self.shard_index(partition)]
	}

	pub(super) fn all_shards(&self) -> &[Mutex<Shard<D>>] {
		&self.inner.shards
	}

	pub(super) fn coverage(&self) -> &RwLock<CoverageIndex<D::Dimension>> {
		&self.inner.coverage
	}

	pub fn retractions(&self) -> u64 {
		self.inner.retractions.token()
	}

	pub(super) fn retractions_unchanged(&self, token: u64) -> bool {
		self.inner.retractions.unchanged(token)
	}

	pub(super) fn record_retraction(&self) {
		self.inner.retractions.record()
	}

	pub(super) fn gap_guard(&self) -> usize {
		self.inner.gap_guard
	}

	pub(super) fn evict_to_capacity(&self, shard: usize) {
		loop {
			let Some((victim, progress)) = self.pick_victim(shard) else {
				break;
			};
			self.retract_partition(&victim);
			#[cfg(test)]
			if let Some(interlock) = self.inner.interlock.as_ref() {
				interlock(self, victim);
			}
			if !self.drop_unpinned(shard, &victim, progress) {
				break;
			}
		}
	}

	fn pick_victim(&self, index: usize) -> Option<(D::Partition, Progress)> {
		let shard = self.shard(index).lock();
		if !shard.budget.over_budget() {
			return None;
		}
		let mut victim: Option<(u64, D::Partition, Progress)> = None;
		for (id, partition) in shard.partitions.iter() {
			if !partition.pinned.has_victim() {
				continue;
			}
			if victim.map(|(tick, _, _)| partition.tick < tick).unwrap_or(true) {
				victim = Some((partition.tick, *id, partition.progress()));
			}
		}
		victim.map(|(_, id, progress)| (id, progress))
	}

	fn retract_partition(&self, victim: &D::Partition) {
		let (start, end) = D::span(victim);
		let mut coverage = self.coverage().write();
		coverage.drop_overlapping(D::dimension(victim), &start, &end);
		self.record_retraction();
	}

	fn drop_unpinned(&self, index: usize, victim: &D::Partition, progress: Progress) -> bool {
		let mut shard = self.shard(index).lock();
		let Shard {
			partitions,
			budget,
			metrics,
			slot_metrics,
			..
		} = &mut *shard;
		let emptied = {
			let Some(partition) = partitions.get_mut(victim) else {
				return true;
			};
			if partition.progress() != progress {
				return false;
			}
			let Partition {
				entries,
				pinned,
				bytes,
				..
			} = partition;
			let mut freed = 0usize;
			entries.retain(|key, entry| {
				if !entry.evictable() {
					return true;
				}
				freed += entry_footprint(key, entry);
				pinned.remove(entry);
				false
			});
			let held = *bytes;
			account(bytes, budget, held, held - freed);
			entries.is_empty()
		};
		if emptied && let Some(gone) = partitions.remove(victim) {
			budget.release(ByteSize::from_bytes(gone.bytes as u64));
		}
		metrics.evictions += 1;
		slot_metrics[D::slot(victim)].evictions += 1;
		true
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	pub fn intervals(&self) -> usize {
		self.coverage().read().intervals()
	}

	pub fn partitions(&self) -> usize {
		self.all_shards().iter().map(|shard| shard.lock().partitions.len()).sum()
	}

	pub fn entries(&self) -> usize {
		self.all_shards()
			.iter()
			.map(|shard| {
				shard.lock().partitions.values().map(|partition| partition.entries.len()).sum::<usize>()
			})
			.sum()
	}

	pub fn shard_limit_bytes(&self) -> ByteSize {
		self.inner.shards[0].lock().budget.limit()
	}

	pub fn metrics(&self) -> RangeMetrics {
		let mut total = RangeMetrics::default();
		for shard in self.all_shards() {
			accumulate(&mut total, &shard.lock().metrics);
		}
		total
	}

	pub fn shard_metrics(&self) -> Vec<RangeShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let entries = shard.partitions.values().map(|partition| partition.entries.len()).sum();
			out.push(RangeShardMetrics {
				shard: index,
				used: shard.budget.used(),
				limit: shard.budget.limit(),
				partitions: shard.partitions.len(),
				entries,
				counters: shard.metrics,
			});
		}
		out
	}

	pub fn slot_metrics(&self) -> Vec<RangeSlotMetrics<D>> {
		let mut used = vec![0u64; D::SLOTS];
		let mut partitions = vec![0usize; D::SLOTS];
		let mut intervals = vec![0usize; D::SLOTS];
		let mut entries = vec![0usize; D::SLOTS];
		let mut counters = vec![RangeMetrics::default(); D::SLOTS];

		let mut resident: Vec<D::Partition> = Vec::new();
		for shard in self.all_shards() {
			let shard = shard.lock();
			for (id, partition) in &shard.partitions {
				let slot = D::slot(id);
				used[slot] += partition.bytes as u64;
				partitions[slot] += 1;
				entries[slot] += partition.entries.len();
				resident.push(*id);
			}
			for (slot, source) in shard.slot_metrics.iter().enumerate() {
				accumulate(&mut counters[slot], source);
			}
		}

		{
			let coverage = self.coverage().read();
			for id in resident {
				let Some(set) = coverage.set(D::dimension(&id)) else {
					continue;
				};
				let (start, end) = D::span(&id);
				intervals[D::slot(&id)] += set.overlapping(&start, &end).len();
			}
		}

		let empty = RangeMetrics::default();
		(0..D::SLOTS)
			.filter(|slot| partitions[*slot] > 0 || counters[*slot] != empty)
			.map(|slot| RangeSlotMetrics {
				slot: D::slot_at(slot),
				used: ByteSize::from_bytes(used[slot]),
				partitions: partitions[slot],
				intervals: intervals[slot],
				entries: entries[slot],
				counters: counters[slot],
			})
			.collect()
	}

	pub fn complete_partitions(&self) -> Vec<usize> {
		let mut resident: Vec<Vec<D::Partition>> = Vec::with_capacity(self.inner.shards.len());
		for shard in self.all_shards() {
			resident.push(shard.lock().partitions.keys().copied().collect());
		}
		let coverage = self.coverage().read();
		resident.iter()
			.map(|ids| {
				ids.iter()
					.filter(|id| {
						let (start, span_end) = D::span(id);
						let end = span_end.min(D::cache_tiers_run_end(id));
						coverage.set(D::dimension(id))
							.and_then(|set| set.covering(&start))
							.is_some_and(|claim| claim.end >= end)
					})
					.count()
			})
			.collect()
	}

	pub fn coverage_bytes(&self) -> ByteSize {
		ByteSize::from_bytes(self.coverage().read().bytes())
	}

	pub fn gap_histogram(&self) -> GapHistogram {
		let mut merged = GapHistogram::new();
		for shard in self.all_shards() {
			merged.merge(&shard.lock().gaps);
		}
		merged
	}

	#[cfg(test)]
	pub fn tallied_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.iter()
			.map(|shard| {
				shard.lock().partitions.values().map(|partition| partition.bytes as u64).sum::<u64>()
			})
			.sum();
		ByteSize::from_bytes(total)
	}
}

impl<D: RangeDomain> MetricsCollector for RangeTier<D> {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(D::SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::heap(D::SCOPE, "coverage_bytes", self.coverage_bytes()));
		out.push(MetricsSample::count(D::SCOPE, "resident_intervals", self.intervals() as u64));
		out.push(MetricsSample::count(D::SCOPE, "resident_partitions", self.partitions() as u64));
		out.push(MetricsSample::count(D::SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(D::SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(D::SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(D::SCOPE, "exempt", counters.exempt));
		out.push(MetricsSample::counter(D::SCOPE, "materializes", counters.materializes));
		out.push(MetricsSample::counter(D::SCOPE, "materializes_refused", counters.materializes_refused));
		out.push(MetricsSample::counter(D::SCOPE, "materializes_raced", counters.materializes_raced));
		out.push(MetricsSample::counter(D::SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(D::SCOPE, "point_hits", counters.point_hits));
		out.push(MetricsSample::counter(D::SCOPE, "point_absences", counters.point_absences));
		out.push(MetricsSample::counter(D::SCOPE, "point_misses", counters.point_misses));
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
			out.push(MetricsSample::count(scope.clone(), "partitions", keyspace.partitions as u64));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope.clone(), "misses", keyspace.counters.misses));
			out.push(MetricsSample::counter(scope.clone(), "exempt", keyspace.counters.exempt));
			out.push(MetricsSample::counter(scope.clone(), "materializes", keyspace.counters.materializes));
			out.push(MetricsSample::counter(scope.clone(), "evictions", keyspace.counters.evictions));
			out.push(MetricsSample::counter(scope.clone(), "point_hits", keyspace.counters.point_hits));
			out.push(MetricsSample::counter(
				scope.clone(),
				"point_absences",
				keyspace.counters.point_absences,
			));
			out.push(MetricsSample::counter(scope, "point_misses", keyspace.counters.point_misses));
		}
		let gaps = self.gap_histogram();
		out.push(MetricsSample::counter(D::GAP_SCOPE, "scans", gaps.scans()));
		out.push(MetricsSample::counter(D::GAP_SCOPE, "degraded", gaps.degraded()));
		for (slot, count) in GAP_SLOTS.iter().zip(gaps.slots().iter()) {
			out.push(MetricsSample::counter(D::GAP_SCOPE, slot, *count));
		}
		if let Some(median) = gaps.median() {
			out.push(MetricsSample::count(D::GAP_SCOPE, "median", median as u64));
		}
	}
}

fn accumulate(target: &mut RangeMetrics, source: &RangeMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.exempt += source.exempt;
	target.materializes += source.materializes;
	target.materializes_refused += source.materializes_refused;
	target.materializes_raced += source.materializes_raced;
	target.evictions += source.evictions;
	target.point_hits += source.point_hits;
	target.point_absences += source.point_absences;
	target.point_misses += source.point_misses;
}

fn build_shards<D: RangeDomain>(config: RangeConfig, shard_bytes: ByteSize) -> Box<[Mutex<Shard<D>>]> {
	let shard_count = config.shards.max(1);
	let byte_cap = ByteSize::from_bytes(shard_bytes.as_bytes().max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				partitions: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				writes: 0,
				gaps: GapHistogram::new(),
				metrics: RangeMetrics::default(),
				slot_metrics: vec![RangeMetrics::default(); D::SLOTS].into_boxed_slice(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering as AtomicOrdering},
	};

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, KeyspaceId, OperatorStateKey},
		metrics::{
			collect::MetricsCollector,
			sample::{MetricsSample, Reading},
		},
		util::sorted::SortedVecMap,
	};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use crate::{
		coverage::{
			entry::{Entry, PinnedCount},
			interval::Interval,
			plan::{DEFAULT_GAP_GUARD, ScanPlan},
		},
		tier::range::{
			Partition, RangeConfig, RangeTier, Shard,
			domain::{TestDomain as D, TestPartition},
			entry_footprint, partition_overhead,
		},
	};

	const PARTITION_OVERHEAD: usize = partition_overhead::<D>();

	const OP_A: OperatorId = OperatorId(1);
	const GROUP_A: GroupId = GroupId(10);

	fn config(limit_bytes: u64, shards: usize) -> RangeConfig {
		RangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(limit_bytes)),
			shards,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}

	fn tier(limit_bytes: u64, shards: usize) -> RangeTier<D> {
		RangeTier::<D>::new(config(limit_bytes, shards)).expect("a tier with a byte budget must be constructed")
	}

	fn roomy() -> RangeTier<D> {
		tier(ByteSize::from_mib(1).as_bytes(), 1)
	}

	fn key(keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP_A, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn part(keyspace: KeyspaceId) -> TestPartition {
		TestPartition {
			dimension: OP_A,
			group: GROUP_A,
			slot: keyspace,
		}
	}

	fn cost(rows: &[(EncodedKey, Entry<EncodedPodRow>)]) -> usize {
		PARTITION_OVERHEAD + rows.iter().map(|(key, entry)| entry_footprint(key, entry)).sum::<usize>()
	}

	fn seed(tier: &RangeTier<D>, id: TestPartition, rows: Vec<(EncodedKey, Entry<EncodedPodRow>)>) {
		// Rows must land before coverage extends, or the fixture claims a span it does not hold.
		let index = tier.shard_index(&id);
		{
			let mut shard = tier.shard(index).lock();
			let tick = shard.next_tick;
			shard.next_tick = tick + 1;
			let Shard {
				partitions,
				budget,
				..
			} = &mut *shard;
			let slot = partitions.entry(id).or_insert_with(|| {
				budget.charge(ByteSize::from_bytes(PARTITION_OVERHEAD as u64));
				Partition {
					entries: SortedVecMap::new(),
					pinned: PinnedCount::new(),
					bytes: PARTITION_OVERHEAD,
					tick,
					created: tick,
					materializes: 0,
					written_at: 0,
					covered: true,
				}
			});
			slot.tick = tick;
			slot.materializes += 1;
			slot.covered = true;
			for (key, entry) in rows {
				let footprint = entry_footprint(&key, &entry);
				slot.pinned.insert(&entry);
				slot.bytes += footprint;
				budget.charge(ByteSize::from_bytes(footprint as u64));
				slot.entries.insert(key, entry);
			}
		}
		let (start, end) = id.span();
		tier.coverage().write().extend(id.dimension, start, end);
	}

	fn resident(tier: &RangeTier<D>, key: &EncodedKey) -> Option<Entry<EncodedPodRow>> {
		let id = TestPartition::of(OP_A, key).expect("a fixture key always names a partition");
		let index = tier.shard_index(&id);
		let shard = tier.shard(index).lock();
		shard.partitions.get(&id).and_then(|partition| partition.entries.get(key).cloned())
	}

	fn probe(tier: &RangeTier<D>, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		// Some(None) is a proven absence and None a fall through; the two must never be confused.
		if let Some(entry) = resident(tier, key) {
			return Some(entry.value().cloned());
		}
		let covered = tier.coverage().read().contains(OP_A, key);
		if covered {
			Some(None)
		} else {
			None
		}
	}

	fn claims(tier: &RangeTier<D>) -> Vec<Interval> {
		tier.coverage().read().set(OP_A).map(|set| set.iter().collect()).unwrap_or_default()
	}

	fn scan_plan(gaps: usize, degraded: bool) -> ScanPlan {
		ScanPlan {
			segments: Vec::new(),
			gaps,
			exempted: 0,
			degraded,
		}
	}

	fn reading(out: &[MetricsSample], scope: &str, metric: &str) -> Option<Reading> {
		out.iter().find(|sample| sample.scope == scope && sample.metric == metric).map(|sample| sample.reading)
	}

	#[test]
	fn eviction_retracts_the_claim_it_drops_the_rows_of() {
		// An evicted key must fall through, never resolve as a proven absence the tier cannot back.
		let k = key(KeyspaceId::ACCUMULATOR, b"a");
		let rows = vec![(k.clone(), Entry::row(row("v")))];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(KeyspaceId::ACCUMULATOR), rows);

		assert_eq!(probe(&tier, &k), Some(Some(row("v"))), "the seeded row must be resident to begin with");

		tier.evict_to_capacity(0);

		assert_eq!(probe(&tier, &k), None, "an evicted key must fall through, never answer proven absent");
		assert_eq!(tier.intervals(), 0, "the claim over the evicted span must be gone");
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.partitions(), 0);
		assert_eq!(tier.metrics().evictions, 1);
		assert_eq!(tier.retractions(), 1, "the shrink must be visible to a materialize still in flight");
	}

	#[test]
	fn eviction_never_drops_an_unflushed_removal() {
		// Dropping a removal the persistent tier has not seen resurrects the row it still holds.
		let live = key(KeyspaceId::ACCUMULATOR, b"a");
		let gone = key(KeyspaceId::ACCUMULATOR, b"b");
		let rows = vec![(live.clone(), Entry::row(row("v"))), (gone.clone(), Entry::deleted())];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(KeyspaceId::ACCUMULATOR), rows);

		tier.evict_to_capacity(0);

		assert_eq!(
			resident(&tier, &gone),
			Some(Entry::Deleted),
			"the unflushed removal must still be resident"
		);
		assert_eq!(tier.entries(), 1, "only the evictable row may be dropped");
		assert_eq!(probe(&tier, &live), None, "the dropped row falls through");
		assert_eq!(tier.partitions(), 1, "a partition holding a pinned entry is never removed");
	}

	#[test]
	fn an_entirely_pinned_partition_is_never_offered_as_a_victim() {
		// The budget loop must stop at an all-pinned floor, or it spins forever on the same partition.
		let first = key(KeyspaceId::ACCUMULATOR, b"a");
		let second = key(KeyspaceId::ACCUMULATOR, b"b");
		let rows = vec![(first, Entry::deleted()), (second, Entry::deleted())];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(KeyspaceId::ACCUMULATOR), rows);

		tier.evict_to_capacity(0);

		assert_eq!(tier.entries(), 2, "nothing was evictable");
		assert_eq!(tier.metrics().evictions, 0, "no partition may be counted as evicted");
		assert_eq!(tier.retractions(), 0, "a claim must not be withdrawn over rows that stay");
		assert_eq!(tier.intervals(), 1, "the surviving removals keep their interval authoritative");
	}

	#[test]
	fn a_materialize_that_reclaims_the_span_stops_the_rows_being_dropped() {
		// A materialize racing the drop must keep its rows, or its fresh claim stands over nothing.
		let k = key(KeyspaceId::ACCUMULATOR, b"a");
		let rows = vec![(k.clone(), Entry::row(row("v")))];
		let fired = Arc::new(AtomicBool::new(false));
		let tier = RangeTier::<D>::with_interlock(
			config(cost(&rows) as u64 - 1, 1),
			Box::new(move |tier, id| {
				if fired.swap(true, AtomicOrdering::SeqCst) {
					return;
				}
				seed(tier, id, vec![(key(KeyspaceId::ACCUMULATOR, b"z"), Entry::row(row("late")))]);
			}),
		)
		.expect("a tier with a byte budget must be constructed");
		seed(&tier, part(KeyspaceId::ACCUMULATOR), rows);

		tier.evict_to_capacity(0);

		assert_eq!(probe(&tier, &k), Some(Some(row("v"))), "the raced rows must survive untouched");
		assert_eq!(tier.entries(), 2, "the materialize's row joined them rather than replacing them");
		assert_eq!(tier.metrics().evictions, 0, "a raced pass evicts nothing");
	}

	#[test]
	fn eviction_releases_exactly_the_bytes_the_insert_charged() {
		// A tally that drifts from the budget wedges the shard over its limit or lets it grow unbounded.
		let hot = key(KeyspaceId::BUFFER, b"b");
		let cold = key(KeyspaceId::ACCUMULATOR, b"a");
		let cold_rows = vec![(cold.clone(), Entry::row(row("aaaaaaaa")))];
		let hot_rows = vec![(hot.clone(), Entry::row(row("b")))];
		let total = cost(&cold_rows) + cost(&hot_rows);
		let tier = tier(total as u64 - 1, 1);

		seed(&tier, part(KeyspaceId::ACCUMULATOR), cold_rows.clone());
		seed(&tier, part(KeyspaceId::BUFFER), hot_rows.clone());

		assert_eq!(tier.resident_bytes(), ByteSize::from_bytes(total as u64));
		assert_eq!(tier.tallied_bytes(), tier.resident_bytes());

		tier.evict_to_capacity(0);

		assert_eq!(
			tier.resident_bytes(),
			ByteSize::from_bytes(cost(&hot_rows) as u64),
			"only the surviving partition may still be charged"
		);
		assert_eq!(tier.tallied_bytes(), tier.resident_bytes(), "the tally must follow the budget");
		assert!(resident(&tier, &hot).is_some(), "the least recently seeded partition is the victim");
	}

	#[test]
	fn eviction_drops_the_claim_that_coalesced_across_two_partitions() {
		// A coalesced claim goes whole on eviction, never splits: splitting adds one interval per
		// eviction and the index then grows without bound. The survivor keeps answering from ram
		// and only forfeits its proven-absence claim until the next scan re-claims the span.
		let cold = key(KeyspaceId::ACCUMULATOR, b"a");
		let hot = key(KeyspaceId::BUFFER, b"b");
		let cold_rows = vec![(cold.clone(), Entry::row(row("v")))];
		let hot_rows = vec![(hot.clone(), Entry::row(row("v")))];
		let tier = tier((cost(&cold_rows) + cost(&hot_rows)) as u64 - 1, 1);

		seed(&tier, part(KeyspaceId::ACCUMULATOR), cold_rows);
		seed(&tier, part(KeyspaceId::BUFFER), hot_rows);
		assert_eq!(tier.intervals(), 1, "two touching partitions must coalesce into one claim");

		tier.evict_to_capacity(0);

		assert!(claims(&tier).is_empty(), "the coalesced claim must go whole");
		assert!(tier.intervals() <= 1, "retraction must never raise the interval count");
		assert_eq!(probe(&tier, &cold), None, "the evicted partition falls through");
		assert_eq!(probe(&tier, &hot), Some(Some(row("v"))), "the survivor still answers from ram");
	}

	#[test]
	fn the_gap_histogram_merges_every_shard_it_was_recorded_on() {
		// A histogram that loses a shard's scans argues for a guard nobody measured.
		let tier = tier(ByteSize::from_mib(1).as_bytes(), 2);
		tier.shard(0).lock().gaps.record(&scan_plan(0, false));
		tier.shard(1).lock().gaps.record(&scan_plan(3, true));

		let merged = tier.gap_histogram();

		assert_eq!(merged.scans(), 2, "both shards must be counted");
		assert_eq!(merged.slots()[0], 1, "the gapless scan lands in the first slot");
		assert_eq!(merged.slots()[3], 1, "the three-gap scan lands in its own slot");
		assert_eq!(merged.degraded(), 1, "the abandoned plan must be visible");
	}

	#[test]
	fn collect_reports_the_shape_signals_under_their_ratified_names() {
		// An old metric name here reports nothing at all to every dashboard keyed on it.
		let tier = roomy();
		seed(
			&tier,
			part(KeyspaceId::ACCUMULATOR),
			vec![(key(KeyspaceId::ACCUMULATOR, b"a"), Entry::row(row("v")))],
		);
		tier.shard(0).lock().gaps.record(&scan_plan(2, false));

		let mut out = Vec::new();
		tier.collect(&mut out);

		assert_eq!(reading(&out, "operator_range", "resident_intervals"), Some(Reading::Count(Count::new(1))));
		assert_eq!(reading(&out, "operator_range", "resident_partitions"), Some(Reading::Count(Count::new(1))));
		assert_eq!(reading(&out, "operator_range", "resident_entries"), Some(Reading::Count(Count::new(1))));
		assert!(reading(&out, "operator_range", "materializes").is_some());
		assert!(reading(&out, "operator_range", "materializes_refused").is_some());
		assert!(reading(&out, "operator_range", "materializes_raced").is_some());
		assert!(reading(&out, "operator_range", "fills").is_none(), "the old name must not survive");
		assert!(reading(&out, "operator_range", "resident_buckets").is_none());
		assert_eq!(reading(&out, "operator_range::gaps", "scans"), Some(Reading::Count(Count::new(1))));
		assert_eq!(reading(&out, "operator_range::gaps", "count_2"), Some(Reading::Count(Count::new(1))));
	}
}
