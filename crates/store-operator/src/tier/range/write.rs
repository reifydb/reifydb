// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_store::coverage::{ExclusiveUpperEnd, entry::Entry, successor};
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::{
	OperatorRangeTier, PARTITION_OVERHEAD, Partition, PartitionId, PinnedCount, Shard, account, entry_footprint,
};

impl OperatorRangeTier {
	pub fn overwrite(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		let Some(partition) = self.cacheable(operator, &key) else {
			return;
		};
		let index = self.shard_index(&partition);
		if self.place(index, &partition, key, Entry::row(row)) {
			self.evict_to_capacity(index);
		}
	}

	pub fn insert(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		let Some(partition) = self.cacheable(operator, &key) else {
			return;
		};
		let index = self.shard_index(&partition);
		if !self.place(index, &partition, key.clone(), Entry::row(row)) {
			return;
		}
		self.claim_island(operator, &key);
		self.evict_to_capacity(index);
	}

	pub fn mark_deleted(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(partition) = self.cacheable(operator, key) else {
			return;
		};
		let index = self.shard_index(&partition);
		if !self.participates(index, &partition) {
			return;
		}
		if self.coverage().read().operators.get(&operator).is_some_and(|set| set.contains(key)) {
			if self.place(index, &partition, key.clone(), Entry::deleted()) {
				self.evict_to_capacity(index);
			}
			return;
		}
		self.withdraw(operator, key);
		self.discard(index, &partition, key);
	}

	pub fn retract(&self, operator: OperatorId, key: &EncodedKey) {
		let Some(partition) = self.cacheable(operator, key) else {
			return;
		};
		let index = self.shard_index(&partition);
		if self.place(index, &partition, key.clone(), Entry::absent()) {
			self.evict_to_capacity(index);
		}
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		{
			let mut coverage = self.coverage().write();
			coverage.operators.remove(&operator);
			self.record_retraction();
		}
		for shard in self.all_shards() {
			let mut shard = shard.lock();
			let Shard {
				partitions,
				budget,
				..
			} = &mut *shard;
			let victims: Vec<PartitionId> =
				partitions.keys().filter(|id| id.operator == operator).copied().collect();
			for victim in victims {
				if let Some(target) = partitions.remove(&victim) {
					budget.release(ByteSize::from_bytes(target.bytes as u64));
				}
			}
		}
	}

	fn cacheable(&self, operator: OperatorId, key: &EncodedKey) -> Option<PartitionId> {
		PartitionId::of(operator, key).filter(PartitionId::caches_ranges)
	}

	fn participates(&self, index: usize, partition: &PartitionId) -> bool {
		self.shard(index).lock().partitions.get(partition).is_some_and(|target| target.covered)
	}

	fn place(&self, index: usize, partition: &PartitionId, key: EncodedKey, entry: Entry<EncodedPodRow>) -> bool {
		let resident = self.shard(index).lock().partitions.get(partition).map(|target| target.covered);
		let admit = match resident {
			Some(covered) => covered,
			None => self.claims(partition, &key),
		};
		if !admit {
			return false;
		}

		let mut shard = self.shard(index).lock();
		let next = shard.next_tick;
		{
			let Shard {
				partitions,
				budget,
				..
			} = &mut *shard;
			let fresh = !partitions.contains_key(partition);
			let target = partitions.entry(*partition).or_insert_with(|| Partition {
				entries: BTreeMap::new(),
				pinned: PinnedCount::new(),
				bytes: PARTITION_OVERHEAD,
				tick: next,
				installs: 0,
				covered: true,
			});
			if fresh {
				budget.charge(ByteSize::from_bytes(PARTITION_OVERHEAD as u64));
			}
			let new = entry_footprint(&key, &entry);
			let old = match target.entries.get(&key) {
				Some(previous) => {
					let old = entry_footprint(&key, previous);
					target.pinned.replace(previous, &entry);
					old
				}
				None => {
					target.pinned.insert(&entry);
					0
				}
			};
			target.entries.insert(key, entry);
			account(&mut target.bytes, budget, old, new);
			target.tick = next;
		}
		shard.next_tick = next + 1;
		true
	}

	fn claims(&self, partition: &PartitionId, key: &EncodedKey) -> bool {
		self.coverage().read().operators.get(&partition.operator).is_some_and(|set| set.contains(key))
	}

	fn discard(&self, index: usize, partition: &PartitionId, key: &EncodedKey) {
		let mut shard = self.shard(index).lock();
		let Shard {
			partitions,
			budget,
			..
		} = &mut *shard;
		let Some(target) = partitions.get_mut(partition) else {
			return;
		};
		let Some(previous) = target.entries.remove(key) else {
			return;
		};
		target.pinned.remove(&previous);
		account(&mut target.bytes, budget, entry_footprint(key, &previous), 0);
	}

	fn withdraw(&self, operator: OperatorId, key: &EncodedKey) {
		let mut coverage = self.coverage().write();
		if let Some(set) = coverage.operators.get_mut(&operator) {
			set.shrink_key(key);
		}
		self.record_retraction();
	}

	fn claim_island(&self, operator: OperatorId, key: &EncodedKey) {
		if self.coverage().read().operators.get(&operator).is_some_and(|set| set.contains(key)) {
			return;
		}
		let mut coverage = self.coverage().write();
		coverage.operators
			.entry(operator)
			.or_default()
			.extend(key.clone(), ExclusiveUpperEnd::Key(successor(key)));
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	};
	use reifydb_store::coverage::{
		ExclusiveUpperEnd,
		entry::{PinnedCount, Residency},
		interval::Interval,
		successor,
	};
	use reifydb_value::byte_size::ByteSize;

	use crate::tier::range::{OperatorRangeConfig, OperatorRangeTier, PARTITION_OVERHEAD, Partition, PartitionId};

	const OP_A: OperatorId = OperatorId(1);
	const OP_B: OperatorId = OperatorId(2);
	const GROUP_A: GroupId = GroupId(10);
	const CACHED: Keyspace = Keyspace::ACCUMULATOR;
	const OTHER: Keyspace = Keyspace::BUFFER;
	const UNCACHED: Keyspace = Keyspace::CUSTOM_NOT_CACHED;

	fn tier() -> OperatorRangeTier {
		OperatorRangeTier::new(OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			..OperatorRangeConfig::default()
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn key(keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP_A, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn partition(operator: OperatorId, keyspace: Keyspace) -> PartitionId {
		PartitionId {
			operator,
			group: GROUP_A,
			keyspace,
		}
	}

	fn participate(tier: &OperatorRangeTier, id: PartitionId) {
		// Seating a second entry must not reset the first, so an existing partition is left alone.
		let mut shard = tier.shard_for(&id).lock();
		if shard.partitions.contains_key(&id) {
			return;
		}
		shard.budget.charge(ByteSize::from_bytes(PARTITION_OVERHEAD as u64));
		shard.partitions.insert(
			id,
			Partition {
				entries: BTreeMap::new(),
				pinned: PinnedCount::new(),
				bytes: PARTITION_OVERHEAD,
				tick: 0,
				installs: 0,
				covered: true,
			},
		);
	}

	fn claim(tier: &OperatorRangeTier, operator: OperatorId, start: &EncodedKey, end: &EncodedKey) {
		tier.coverage()
			.write()
			.operators
			.entry(operator)
			.or_default()
			.extend(start.clone(), ExclusiveUpperEnd::Key(end.clone()));
	}

	fn residency(tier: &OperatorRangeTier, id: &PartitionId, at: &EncodedKey) -> Option<Residency<EncodedPodRow>> {
		tier.shard_for(id)
			.lock()
			.partitions
			.get(id)
			.and_then(|target| target.entries.get(at))
			.map(|entry| entry.residency.clone())
	}

	fn pinned(tier: &OperatorRangeTier, id: &PartitionId) -> PinnedCount {
		tier.shard_for(id).lock().partitions.get(id).map(|target| target.pinned).unwrap_or_default()
	}

	fn bytes(tier: &OperatorRangeTier, id: &PartitionId) -> usize {
		tier.shard_for(id).lock().partitions.get(id).map(|target| target.bytes).unwrap_or(0)
	}

	fn has_partition(tier: &OperatorRangeTier, id: &PartitionId) -> bool {
		tier.shard_for(id).lock().partitions.contains_key(id)
	}

	fn resident_entries(tier: &OperatorRangeTier) -> usize {
		tier.all_shards()
			.iter()
			.map(|shard| shard.lock().partitions.values().map(|target| target.entries.len()).sum::<usize>())
			.sum()
	}

	fn covers(tier: &OperatorRangeTier, operator: OperatorId, at: &EncodedKey) -> bool {
		tier.coverage().read().operators.get(&operator).is_some_and(|set| set.contains(at))
	}

	fn intervals(tier: &OperatorRangeTier, operator: OperatorId) -> Vec<Interval> {
		tier.coverage().read().operators.get(&operator).map(|set| set.iter().collect()).unwrap_or_default()
	}

	fn island(at: &EncodedKey) -> Interval {
		Interval::new(at.clone(), ExclusiveUpperEnd::Key(successor(at)))
	}

	#[test]
	fn overwrite_into_a_partition_taking_part_in_no_claim_stores_nothing() {
		// A row placed where nothing was scanned is a claim no one proved, charged for no read.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");

		tier.overwrite(OP_A, at.clone(), row("v"));

		assert!(!has_partition(&tier, &id), "an ignored overwrite must not conjure a partition");
		assert_eq!(residency(&tier, &id, &at), None);
		assert!(intervals(&tier, OP_A).is_empty());
	}

	#[test]
	fn overwrite_never_grows_a_claim() {
		// Covering the key says nothing about its neighbours and yields one interval per flushed row.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);

		tier.overwrite(OP_A, at.clone(), row("v"));

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Row(row("v"))));
		assert!(intervals(&tier, OP_A).is_empty(), "an overwrite claimed a span the writer never observed");
		assert!(!covers(&tier, OP_A, &at));
	}

	#[test]
	fn overwrite_replaces_a_row_and_charges_only_the_difference() {
		// Charging the whole new footprint again leaks budget until a live working set is evicted.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);

		tier.overwrite(OP_A, at.clone(), row("v"));
		let one = bytes(&tier, &id);
		tier.overwrite(OP_A, at.clone(), row("w"));

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Row(row("w"))));
		assert_eq!(bytes(&tier, &id), one, "an equal-sized replacement must not move the tally");
		assert_eq!(pinned(&tier, &id).total(), 1, "a replacement is one entry, not two");
	}

	#[test]
	fn insert_into_an_uncovered_gap_makes_a_one_key_island_that_does_not_widen() {
		// Widening would claim a proven absence over neighbours no one has ever observed.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);

		tier.insert(OP_A, at.clone(), row("v"));

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Row(row("v"))));
		assert_eq!(intervals(&tier, OP_A), vec![island(&at)]);
		assert!(!covers(&tier, OP_A, &key(CACHED, b"l")), "the island widened below its key");
		assert!(!covers(&tier, OP_A, &key(CACHED, b"n")), "the island widened above its key");
	}

	#[test]
	fn insert_inside_a_claim_joins_it_without_fragmenting_it() {
		// Re-cutting a claim already covering the key costs a round trip per written key next scan.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, OP_A, &key(CACHED, b"a"), &key(CACHED, b"z"));
		let before = intervals(&tier, OP_A);

		tier.insert(OP_A, at.clone(), row("v"));

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Row(row("v"))));
		assert_eq!(intervals(&tier, OP_A), before, "an insert inside a claim must leave it alone");
	}

	#[test]
	fn insert_into_a_partition_taking_part_in_no_claim_claims_nothing() {
		// Islanding inserts into never-scanned partitions makes a scan pay one gap per row.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");

		tier.insert(OP_A, at.clone(), row("v"));

		assert_eq!(residency(&tier, &id, &at), None);
		assert!(intervals(&tier, OP_A).is_empty());
	}

	#[test]
	fn mark_deleted_inside_a_claim_stores_a_pinned_removal_it_charges_for() {
		// The persistent tier still holds the row, so the entry must never read as a proven absence.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, OP_A, &key(CACHED, b"a"), &key(CACHED, b"z"));
		tier.overwrite(OP_A, at.clone(), row("v"));
		let before = tier.retractions();

		tier.mark_deleted(OP_A, &at);

		assert_eq!(
			residency(&tier, &id, &at),
			Some(Residency::Deleted),
			"a removal the flush has not seen must never be stored as a proven absence"
		);
		assert_eq!(pinned(&tier, &id).pinned(), 1);
		assert!(!pinned(&tier, &id).has_victim(), "eviction must not be offered an unflushed removal");
		assert!(bytes(&tier, &id) > PARTITION_OVERHEAD, "the removal must stay charged to the budget");
		assert_eq!(tier.lookup(OP_A, &at), Some(None), "a removal answers a read outright");
		assert!(covers(&tier, OP_A, &at), "the claim around a removal stays standing");
		assert_eq!(tier.retractions(), before, "a removal inside a claim withdraws nothing");
	}

	#[test]
	fn mark_deleted_outside_a_claim_shrinks_coverage_instead_of_storing_an_absence() {
		// Without the retraction bump an install in flight never learns the removal happened.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		tier.overwrite(OP_A, at.clone(), row("v"));
		let before = tier.retractions();

		tier.mark_deleted(OP_A, &at);

		assert_eq!(residency(&tier, &id, &at), None, "a removal outside a claim must store nothing");
		assert!(
			tier.retractions() > before,
			"an install reading the persistent tier right now would reinstate the removed row"
		);
		assert_eq!(tier.lookup(OP_A, &at), None, "an uncovered removal must fall through");
		assert_eq!(pinned(&tier, &id), PinnedCount::new());
		assert_eq!(bytes(&tier, &id), PARTITION_OVERHEAD, "the dropped row must be released");
	}

	#[test]
	fn mark_deleted_in_a_partition_taking_part_in_no_claim_touches_nothing() {
		// Taking the one global coverage lock per removal would serialise every write in the system.
		let tier = tier();
		let at = key(CACHED, b"m");
		let before = tier.retractions();

		tier.mark_deleted(OP_A, &at);

		assert_eq!(tier.retractions(), before);
		assert!(!has_partition(&tier, &partition(OP_A, CACHED)));
	}

	#[test]
	fn retract_leaves_a_proven_absence_where_a_removal_stood() {
		// The key must stay resident, or an in-flight scan reinstates the row it read before the flush.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, OP_A, &key(CACHED, b"a"), &key(CACHED, b"z"));
		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.mark_deleted(OP_A, &at);

		tier.retract(OP_A, &at);

		assert_eq!(
			residency(&tier, &id, &at),
			Some(Residency::Absent),
			"an erased key lets a stale in-flight row back in under a fresh claim"
		);
		assert_eq!(pinned(&tier, &id).pinned(), 0, "a flushed removal must be reclaimable");
		assert!(pinned(&tier, &id).has_victim());
		assert_eq!(pinned(&tier, &id).total(), 1, "the key is still one entry, not zero");
		assert_eq!(tier.lookup(OP_A, &at), Some(None));
	}

	#[test]
	fn retract_leaves_a_proven_absence_where_no_entry_stood() {
		// Without the entry the next install places the row it read before the flush.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);

		tier.retract(OP_A, &at);

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Absent));
		assert_eq!(pinned(&tier, &id).total(), 1);
	}

	#[test]
	fn retract_replaces_a_row_the_flush_removed() {
		// Leaving the row resident would serve data the persistent tier no longer holds.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		tier.overwrite(OP_A, at.clone(), row("v"));

		tier.retract(OP_A, &at);

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Absent));
		assert_eq!(tier.lookup(OP_A, &at), Some(None));
	}

	#[test]
	fn retract_in_a_partition_taking_part_in_no_claim_stores_nothing() {
		// Nothing here is answerable, so a proven absence would only cost budget.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");

		tier.retract(OP_A, &at);

		assert!(!has_partition(&tier, &id));
	}

	#[test]
	fn overwrite_of_a_removal_restores_the_row_and_releases_its_pin() {
		// A rewrite moves one entry across the pin; a leaked pin leaves a floor eviction never clears.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, OP_A, &key(CACHED, b"a"), &key(CACHED, b"z"));
		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.mark_deleted(OP_A, &at);
		assert_eq!(pinned(&tier, &id).pinned(), 1);

		tier.overwrite(OP_A, at.clone(), row("w"));

		assert_eq!(residency(&tier, &id, &at), Some(Residency::Row(row("w"))));
		assert_eq!((pinned(&tier, &id).pinned(), pinned(&tier, &id).total()), (0, 1));
		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("w"))));
	}

	#[test]
	fn invalidate_operator_clears_claims_partitions_entries_and_pinned_removals() {
		// A surviving removal or claim answers for a keyspace that is gone and stays charged forever.
		let tier = tier();
		let live = partition(OP_A, CACHED);
		let second = partition(OP_A, OTHER);
		let other_operator = partition(OP_B, CACHED);
		let at = key(CACHED, b"m");
		let elsewhere = key(OTHER, b"m");
		for id in [live, second, other_operator] {
			participate(&tier, id);
		}
		claim(&tier, OP_A, &key(CACHED, b"a"), &key(CACHED, b"z"));
		claim(&tier, OP_A, &key(OTHER, b"a"), &key(OTHER, b"z"));
		claim(&tier, OP_B, &key(CACHED, b"a"), &key(CACHED, b"z"));
		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.overwrite(OP_A, elsewhere.clone(), row("v"));
		tier.overwrite(OP_B, at.clone(), row("v"));
		tier.mark_deleted(OP_A, &elsewhere);
		assert_eq!(pinned(&tier, &second).pinned(), 1);
		let charged = tier.shard_for(&live).lock().budget.used();
		let before = tier.retractions();

		tier.invalidate_operator(OP_A);

		assert!(intervals(&tier, OP_A).is_empty(), "a purged operator must hold no claim");
		assert!(!has_partition(&tier, &live));
		assert!(!has_partition(&tier, &second), "a partition holding a pinned removal must go too");
		assert!(tier.retractions() > before, "an install in flight against the purge must refuse");
		assert!(
			tier.shard_for(&live).lock().budget.used() < charged,
			"a purge that never releases its bytes starves every other partition"
		);
		assert!(has_partition(&tier, &other_operator), "another operator must be untouched");
		assert_eq!(residency(&tier, &other_operator, &at), Some(Residency::Row(row("v"))));
		assert!(covers(&tier, OP_B, &at));
	}

	fn seat_unclaimed(tier: &OperatorRangeTier, id: PartitionId) {
		// The partition must exist yet stay uncovered, or the write paths under test are not reached.
		let mut shard = tier.shard_for(&id).lock();
		shard.partitions.insert(
			id,
			Partition {
				entries: BTreeMap::new(),
				pinned: PinnedCount::new(),
				bytes: PARTITION_OVERHEAD,
				tick: 0,
				installs: 0,
				covered: false,
			},
		);
	}

	#[test]
	fn a_partition_that_never_took_part_in_a_claim_is_ignored_by_every_write() {
		// Honouring a write here stores a row nothing ever proved, and pays the global lock to do it.
		let tier = tier();
		let id = partition(OP_A, CACHED);
		let at = key(CACHED, b"m");
		seat_unclaimed(&tier, id);
		let before = tier.retractions();

		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.insert(OP_A, at.clone(), row("v"));
		tier.retract(OP_A, &at);
		tier.mark_deleted(OP_A, &at);

		assert_eq!(residency(&tier, &id, &at), None);
		assert!(intervals(&tier, OP_A).is_empty());
		assert_eq!(tier.retractions(), before);
	}

	#[test]
	fn every_write_declines_a_keyspace_that_is_never_cached() {
		// An uncacheable keyspace in RAM makes its gaps count and degrades every group-wide scan.
		let tier = tier();
		let id = partition(OP_A, UNCACHED);
		let at = key(UNCACHED, b"m");
		participate(&tier, id);
		claim(&tier, OP_A, &key(UNCACHED, b"a"), &key(UNCACHED, b"z"));
		let before = tier.retractions();

		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.insert(OP_A, at.clone(), row("v"));
		tier.retract(OP_A, &at);
		tier.mark_deleted(OP_A, &at);

		assert_eq!(residency(&tier, &id, &at), None);
		assert_eq!(tier.retractions(), before);
	}

	#[test]
	fn every_write_declines_a_key_too_short_to_name_a_partition() {
		// The tier cannot tell which claim such a key belongs to, so caching attaches it arbitrarily.
		let tier = tier();
		let at = EncodedKey::new(b"short");
		let before = tier.retractions();

		tier.overwrite(OP_A, at.clone(), row("v"));
		tier.insert(OP_A, at.clone(), row("v"));
		tier.retract(OP_A, &at);
		tier.mark_deleted(OP_A, &at);

		assert_eq!(resident_entries(&tier), 0);
		assert_eq!(tier.retractions(), before);
	}
}
