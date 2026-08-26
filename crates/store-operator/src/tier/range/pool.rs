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
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_store::coverage::{plan::GapHistogram, retraction::Retractions};
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::range::MaterializeInterlock;
use crate::tier::range::{
	CoverageIndex, KEYSPACE_SLOTS, OperatorRangeConfig, OperatorRangeKeyspaceMetrics, OperatorRangeMetrics,
	OperatorRangeShardMetrics, OperatorRangeTier, Partition, PartitionId, PoolInner, Shard, account,
	entry_footprint,
};

const RANGE_SCOPE: &str = "operator_range";

const GAP_SCOPE: &str = "operator_range::gaps";

const GAP_SLOTS: [&str; 8] =
	["count_0", "count_1", "count_2", "count_3", "count_4", "count_5_8", "count_9_16", "count_17_plus"];

impl OperatorRangeTier {
	pub fn new(config: OperatorRangeConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				coverage: RwLock::new(CoverageIndex {
					operators: HashMap::new(),
				}),
				retractions: Retractions::new(),
				gap_guard: config.gap_guard,
				#[cfg(test)]
				interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(crate) fn with_interlock(config: OperatorRangeConfig, interlock: MaterializeInterlock) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				coverage: RwLock::new(CoverageIndex {
					operators: HashMap::new(),
				}),
				retractions: Retractions::new(),
				gap_guard: config.gap_guard,
				interlock: Some(interlock),
			}),
		})
	}

	pub(super) fn shard_index(&self, partition: &PartitionId) -> usize {
		let mut hasher = DefaultHasher::new();
		partition.hash(&mut hasher);
		(hasher.finish() % self.inner.shards.len() as u64) as usize
	}

	pub(super) fn shard(&self, index: usize) -> &Mutex<Shard> {
		&self.inner.shards[index]
	}

	pub(super) fn shard_for(&self, partition: &PartitionId) -> &Mutex<Shard> {
		&self.inner.shards[self.shard_index(partition)]
	}

	pub(super) fn all_shards(&self) -> &[Mutex<Shard>] {
		&self.inner.shards
	}

	pub(super) fn coverage(&self) -> &RwLock<CoverageIndex> {
		&self.inner.coverage
	}

	pub(super) fn retractions(&self) -> u64 {
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
			let Some((victim, materializes)) = self.pick_victim(shard) else {
				break;
			};
			self.retract_partition(&victim);
			#[cfg(test)]
			if let Some(interlock) = self.inner.interlock.as_ref() {
				interlock(self, victim);
			}
			if !self.drop_unpinned(shard, &victim, materializes) {
				break;
			}
		}
	}

	fn pick_victim(&self, index: usize) -> Option<(PartitionId, u64)> {
		let shard = self.shard(index).lock();
		if !shard.budget.over_budget() {
			return None;
		}
		let mut victim: Option<(u64, PartitionId, u64)> = None;
		for (id, partition) in shard.partitions.iter() {
			if !partition.pinned.has_victim() {
				continue;
			}
			if victim.map(|(tick, _, _)| partition.tick < tick).unwrap_or(true) {
				victim = Some((partition.tick, *id, partition.materializes));
			}
		}
		victim.map(|(_, id, materializes)| (id, materializes))
	}

	fn retract_partition(&self, victim: &PartitionId) {
		let (start, end) = victim.span();
		let mut coverage = self.coverage().write();
		let emptied = match coverage.operators.get_mut(&victim.operator) {
			Some(set) => {
				set.shrink_range(&start, &end);
				set.is_empty()
			}
			None => false,
		};
		if emptied {
			coverage.operators.remove(&victim.operator);
		}
		self.record_retraction();
	}

	fn drop_unpinned(&self, index: usize, victim: &PartitionId, materializes: u64) -> bool {
		let mut shard = self.shard(index).lock();
		let Shard {
			partitions,
			budget,
			metrics,
			keyspace_metrics,
			..
		} = &mut *shard;
		let emptied = {
			let Some(partition) = partitions.get_mut(victim) else {
				return true;
			};
			if partition.materializes != materializes {
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
		keyspace_metrics[victim.keyspace.0 as usize].evictions += 1;
		true
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
	}

	pub fn intervals(&self) -> usize {
		self.coverage().read().operators.values().map(|set| set.len()).sum()
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
			let entries = shard.partitions.values().map(|partition| partition.entries.len()).sum();
			out.push(OperatorRangeShardMetrics {
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

	pub fn keyspace_metrics(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		let mut used = vec![0u64; KEYSPACE_SLOTS];
		let mut partitions = vec![0usize; KEYSPACE_SLOTS];
		let mut intervals = vec![0usize; KEYSPACE_SLOTS];
		let mut entries = vec![0usize; KEYSPACE_SLOTS];
		let mut counters = vec![OperatorRangeMetrics::default(); KEYSPACE_SLOTS];

		let mut resident: Vec<PartitionId> = Vec::new();
		for shard in self.all_shards() {
			let shard = shard.lock();
			for (id, partition) in &shard.partitions {
				let slot = id.keyspace.0 as usize;
				used[slot] += partition.bytes as u64;
				partitions[slot] += 1;
				entries[slot] += partition.entries.len();
				resident.push(*id);
			}
			for (slot, source) in shard.keyspace_metrics.iter().enumerate() {
				accumulate(&mut counters[slot], source);
			}
		}

		{
			let coverage = self.coverage().read();
			for id in resident {
				let Some(set) = coverage.operators.get(&id.operator) else {
					continue;
				};
				let (start, end) = id.span();
				intervals[id.keyspace.0 as usize] += set.overlapping(&start, &end).len();
			}
		}

		let empty = OperatorRangeMetrics::default();
		(0..KEYSPACE_SLOTS)
			.filter(|slot| partitions[*slot] > 0 || counters[*slot] != empty)
			.map(|slot| OperatorRangeKeyspaceMetrics {
				keyspace: Keyspace(slot as u8),
				used: ByteSize::from_bytes(used[slot]),
				partitions: partitions[slot],
				intervals: intervals[slot],
				entries: entries[slot],
				counters: counters[slot],
			})
			.collect()
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

impl MetricsCollector for OperatorRangeTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(RANGE_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(RANGE_SCOPE, "resident_intervals", self.intervals() as u64));
		out.push(MetricsSample::count(RANGE_SCOPE, "resident_partitions", self.partitions() as u64));
		out.push(MetricsSample::count(RANGE_SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(RANGE_SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(RANGE_SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(RANGE_SCOPE, "materializes", counters.materializes));
		out.push(MetricsSample::counter(RANGE_SCOPE, "materializes_refused", counters.materializes_refused));
		out.push(MetricsSample::counter(RANGE_SCOPE, "materializes_raced", counters.materializes_raced));
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
			out.push(MetricsSample::count(scope.clone(), "partitions", keyspace.partitions as u64));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope.clone(), "misses", keyspace.counters.misses));
			out.push(MetricsSample::counter(scope.clone(), "materializes", keyspace.counters.materializes));
			out.push(MetricsSample::counter(scope.clone(), "evictions", keyspace.counters.evictions));
			out.push(MetricsSample::counter(scope.clone(), "point_hits", keyspace.counters.point_hits));
			out.push(MetricsSample::counter(scope, "point_misses", keyspace.counters.point_misses));
		}
		let gaps = self.gap_histogram();
		out.push(MetricsSample::counter(GAP_SCOPE, "scans", gaps.scans()));
		out.push(MetricsSample::counter(GAP_SCOPE, "degraded", gaps.degraded()));
		for (slot, count) in GAP_SLOTS.iter().zip(gaps.slots().iter()) {
			out.push(MetricsSample::counter(GAP_SCOPE, slot, *count));
		}
		if let Some(median) = gaps.median() {
			out.push(MetricsSample::count(GAP_SCOPE, "median", median as u64));
		}
	}
}

fn accumulate(target: &mut OperatorRangeMetrics, source: &OperatorRangeMetrics) {
	target.hits += source.hits;
	target.misses += source.misses;
	target.materializes += source.materializes;
	target.materializes_refused += source.materializes_refused;
	target.materializes_raced += source.materializes_raced;
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
				partitions: HashMap::new(),
				budget: MemoryBudget::new(byte_cap),
				next_tick: 0,
				gaps: GapHistogram::new(),
				metrics: OperatorRangeMetrics::default(),
				keyspace_metrics: Box::new([OperatorRangeMetrics::default(); KEYSPACE_SLOTS]),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering as AtomicOrdering},
		},
	};

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey},
		metrics::{
			collect::MetricsCollector,
			sample::{MetricsSample, Reading},
		},
	};
	use reifydb_store::coverage::{
		entry::{Entry, PinnedCount},
		interval::Interval,
		plan::{DEFAULT_GAP_GUARD, ScanPlan},
	};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use crate::tier::range::{
		OperatorRangeConfig, OperatorRangeTier, PARTITION_OVERHEAD, Partition, PartitionId, Shard,
		entry_footprint,
	};

	const OP_A: OperatorId = OperatorId(1);
	const GROUP_A: GroupId = GroupId(10);

	fn config(limit_bytes: u64, shards: usize) -> OperatorRangeConfig {
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_bytes(limit_bytes)),
			shards,
			gap_guard: DEFAULT_GAP_GUARD,
		}
	}

	fn tier(limit_bytes: u64, shards: usize) -> OperatorRangeTier {
		OperatorRangeTier::new(config(limit_bytes, shards))
			.expect("a tier with a byte budget must be constructed")
	}

	fn roomy() -> OperatorRangeTier {
		tier(ByteSize::from_mib(1).as_bytes(), 1)
	}

	fn key(keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP_A, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn part(keyspace: Keyspace) -> PartitionId {
		PartitionId {
			operator: OP_A,
			group: GROUP_A,
			keyspace,
		}
	}

	fn cost(rows: &[(EncodedKey, Entry<EncodedPodRow>)]) -> usize {
		PARTITION_OVERHEAD + rows.iter().map(|(key, entry)| entry_footprint(key, entry)).sum::<usize>()
	}

	fn seed(tier: &OperatorRangeTier, id: PartitionId, rows: Vec<(EncodedKey, Entry<EncodedPodRow>)>) {
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
					entries: BTreeMap::new(),
					pinned: PinnedCount::new(),
					bytes: PARTITION_OVERHEAD,
					tick,
					materializes: 0,
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
		tier.coverage().write().operators.entry(id.operator).or_default().extend(start, end);
	}

	fn resident(tier: &OperatorRangeTier, key: &EncodedKey) -> Option<Entry<EncodedPodRow>> {
		let id = PartitionId::of(OP_A, key).expect("a fixture key always names a partition");
		let index = tier.shard_index(&id);
		let shard = tier.shard(index).lock();
		shard.partitions.get(&id).and_then(|partition| partition.entries.get(key).cloned())
	}

	fn probe(tier: &OperatorRangeTier, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		// Some(None) is a proven absence and None a fall through; the two must never be confused.
		if let Some(entry) = resident(tier, key) {
			return Some(entry.value().cloned());
		}
		let covered = tier.coverage().read().operators.get(&OP_A).is_some_and(|set| set.contains(key));
		if covered {
			Some(None)
		} else {
			None
		}
	}

	fn claims(tier: &OperatorRangeTier) -> Vec<Interval> {
		tier.coverage().read().operators.get(&OP_A).map(|set| set.iter().collect()).unwrap_or_default()
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
		let k = key(Keyspace::ACCUMULATOR, b"a");
		let rows = vec![(k.clone(), Entry::row(row("v")))];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(Keyspace::ACCUMULATOR), rows);

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
		let live = key(Keyspace::ACCUMULATOR, b"a");
		let gone = key(Keyspace::ACCUMULATOR, b"b");
		let rows = vec![(live.clone(), Entry::row(row("v"))), (gone.clone(), Entry::deleted())];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(Keyspace::ACCUMULATOR), rows);

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
		let first = key(Keyspace::ACCUMULATOR, b"a");
		let second = key(Keyspace::ACCUMULATOR, b"b");
		let rows = vec![(first, Entry::deleted()), (second, Entry::deleted())];
		let tier = tier(cost(&rows) as u64 - 1, 1);
		seed(&tier, part(Keyspace::ACCUMULATOR), rows);

		tier.evict_to_capacity(0);

		assert_eq!(tier.entries(), 2, "nothing was evictable");
		assert_eq!(tier.metrics().evictions, 0, "no partition may be counted as evicted");
		assert_eq!(tier.retractions(), 0, "a claim must not be withdrawn over rows that stay");
		assert_eq!(tier.intervals(), 1, "the surviving removals keep their interval authoritative");
	}

	#[test]
	fn a_materialize_that_reclaims_the_span_stops_the_rows_being_dropped() {
		// A materialize racing the drop must keep its rows, or its fresh claim stands over nothing.
		let k = key(Keyspace::ACCUMULATOR, b"a");
		let rows = vec![(k.clone(), Entry::row(row("v")))];
		let fired = Arc::new(AtomicBool::new(false));
		let tier = OperatorRangeTier::with_interlock(
			config(cost(&rows) as u64 - 1, 1),
			Box::new(move |tier, id| {
				if fired.swap(true, AtomicOrdering::SeqCst) {
					return;
				}
				seed(tier, id, vec![(key(Keyspace::ACCUMULATOR, b"z"), Entry::row(row("late")))]);
			}),
		)
		.expect("a tier with a byte budget must be constructed");
		seed(&tier, part(Keyspace::ACCUMULATOR), rows);

		tier.evict_to_capacity(0);

		assert_eq!(probe(&tier, &k), Some(Some(row("v"))), "the raced rows must survive untouched");
		assert_eq!(tier.entries(), 2, "the materialize's row joined them rather than replacing them");
		assert_eq!(tier.metrics().evictions, 0, "a raced pass evicts nothing");
	}

	#[test]
	fn eviction_releases_exactly_the_bytes_the_insert_charged() {
		// A tally that drifts from the budget wedges the shard over its limit or lets it grow unbounded.
		let hot = key(Keyspace::BUFFER, b"b");
		let cold = key(Keyspace::ACCUMULATOR, b"a");
		let cold_rows = vec![(cold.clone(), Entry::row(row("aaaaaaaa")))];
		let hot_rows = vec![(hot.clone(), Entry::row(row("b")))];
		let total = cost(&cold_rows) + cost(&hot_rows);
		let tier = tier(total as u64 - 1, 1);

		seed(&tier, part(Keyspace::ACCUMULATOR), cold_rows.clone());
		seed(&tier, part(Keyspace::BUFFER), hot_rows.clone());

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
	fn eviction_splits_a_claim_that_coalesced_across_two_partitions() {
		// A coalesced claim must split on eviction, or it covers the evicted partition or loses the survivor.
		let cold = key(Keyspace::ACCUMULATOR, b"a");
		let hot = key(Keyspace::BUFFER, b"b");
		let cold_rows = vec![(cold.clone(), Entry::row(row("v")))];
		let hot_rows = vec![(hot.clone(), Entry::row(row("v")))];
		let tier = tier((cost(&cold_rows) + cost(&hot_rows)) as u64 - 1, 1);

		seed(&tier, part(Keyspace::ACCUMULATOR), cold_rows);
		seed(&tier, part(Keyspace::BUFFER), hot_rows);
		assert_eq!(tier.intervals(), 1, "two touching partitions must coalesce into one claim");

		tier.evict_to_capacity(0);

		let (start, end) = part(Keyspace::BUFFER).span();
		assert_eq!(claims(&tier), vec![Interval::new(start, end)], "only the survivor stays claimed");
		assert_eq!(probe(&tier, &cold), None, "the evicted partition falls through");
		assert_eq!(probe(&tier, &hot), Some(Some(row("v"))), "the survivor still answers");
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
			part(Keyspace::ACCUMULATOR),
			vec![(key(Keyspace::ACCUMULATOR, b"a"), Entry::row(row("v")))],
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
