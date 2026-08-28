// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
use std::cell::RefCell;

use reifydb_codec::key::encoded::EncodedKey;

use crate::tier::range::{RangeDomain, RangeTier, Shard};

#[cfg(test)]
thread_local! {
	static ABSENCE_INTERLOCK: RefCell<Option<Box<dyn Fn()>>> =
		const { RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn arm_absence_interlock(hook: impl Fn() + 'static) {
	ABSENCE_INTERLOCK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn absence_interlock() {
	let hook = ABSENCE_INTERLOCK.with(|slot| slot.borrow_mut().take());
	if let Some(hook) = hook {
		hook();
	}
}

impl<D: RangeDomain> RangeTier<D> {
	pub fn lookup(&self, dimension: D::Dimension, key: &EncodedKey) -> Option<Option<D::Row>> {
		let partition = D::partition(dimension, key)?;
		if !D::caches_ranges(&partition) {
			return None;
		}
		let index = self.shard_index(&partition);
		if let Some(resident) = self.resolve(index, &partition, key) {
			return Some(resident);
		}

		let before = self.retractions();
		let claimed = self.coverage().read().contains(dimension, key);
		if !claimed {
			self.record_point_miss(index, &partition);
			return None;
		}
		#[cfg(test)]
		absence_interlock();
		if let Some(resident) = self.resolve(index, &partition, key) {
			return Some(resident);
		}
		if !self.retractions_unchanged(before) {
			self.record_point_miss(index, &partition);
			return None;
		}
		self.record_point_hit(index, &partition);
		Some(None)
	}

	fn resolve(&self, index: usize, partition: &D::Partition, key: &EncodedKey) -> Option<Option<D::Row>> {
		let mut shard = self.shard(index).lock();
		let next = shard.next_tick;
		let slot = D::slot(partition);
		let mut answer = None;
		{
			let Shard {
				partitions,
				metrics,
				slot_metrics,
				..
			} = &mut *shard;
			if let Some(target) = partitions.get_mut(partition)
				&& let Some(entry) = target.entries.get(key)
			{
				answer = Some(entry.value().cloned());
				target.tick = next;
				metrics.point_hits += 1;
				slot_metrics[slot].point_hits += 1;
			}
		}
		if answer.is_some() {
			shard.next_tick = next + 1;
		}
		answer
	}

	fn record_point_hit(&self, index: usize, partition: &D::Partition) {
		let mut shard = self.shard(index).lock();
		let slot = D::slot(partition);
		shard.metrics.point_hits += 1;
		shard.slot_metrics[slot].point_hits += 1;
	}

	fn record_point_miss(&self, index: usize, partition: &D::Partition) {
		let mut shard = self.shard(index).lock();
		let slot = D::slot(partition);
		shard.metrics.point_misses += 1;
		shard.slot_metrics[slot].point_misses += 1;
	}
}

#[cfg(test)]
mod tests {

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey},
		util::sorted::SortedVecMap,
	};
	use reifydb_value::byte_size::ByteSize;

	use super::arm_absence_interlock;
	use crate::{
		coverage::{
			ExclusiveUpperEnd,
			entry::{Entry, PinnedCount},
		},
		tier::range::{
			Partition, RangeConfig, RangeTier,
			domain::{TestDomain as D, TestPartition},
			entry_footprint, partition_overhead,
		},
	};

	const PARTITION_OVERHEAD: usize = partition_overhead::<D>();

	const OP_A: OperatorId = OperatorId(1);
	const GROUP_A: GroupId = GroupId(10);
	const CACHED: Keyspace = Keyspace::ACCUMULATOR;
	const UNCACHED: Keyspace = Keyspace::CUSTOM_NOT_CACHED;

	fn tier() -> RangeTier<D> {
		RangeTier::<D>::new(RangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			..RangeConfig::testing()
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn key(keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP_A, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn partition(keyspace: Keyspace) -> TestPartition {
		TestPartition {
			dimension: OP_A,
			group: GROUP_A,
			slot: keyspace,
		}
	}

	fn participate(tier: &RangeTier<D>, id: TestPartition) {
		// Seating a second entry must not reset the first, so an existing partition is left alone.
		let mut shard = tier.shard_for(&id).lock();
		if shard.partitions.contains_key(&id) {
			return;
		}
		shard.budget.charge(ByteSize::from_bytes(PARTITION_OVERHEAD as u64));
		shard.partitions.insert(
			id,
			Partition {
				entries: SortedVecMap::new(),
				pinned: PinnedCount::new(),
				bytes: PARTITION_OVERHEAD,
				tick: 0,
				created: 0,
				materializes: 0,
				written_at: 0,
				covered: true,
			},
		);
	}

	fn seat(tier: &RangeTier<D>, id: TestPartition, at: &EncodedKey, entry: Entry<EncodedPodRow>) {
		participate(tier, id);
		let mut shard = tier.shard_for(&id).lock();
		let charged = entry_footprint(at, &entry);
		shard.budget.charge(ByteSize::from_bytes(charged as u64));
		let target = shard.partitions.get_mut(&id).expect("the partition was just seated");
		target.pinned.insert(&entry);
		target.bytes += charged;
		target.entries.insert(at.clone(), entry);
	}

	fn claim(tier: &RangeTier<D>, start: &EncodedKey, end: &EncodedKey) {
		tier.coverage().write().extend(OP_A, start.clone(), ExclusiveUpperEnd::Key(end.clone()));
	}

	fn point_hits(tier: &RangeTier<D>, id: &TestPartition) -> u64 {
		tier.shard_for(id).lock().metrics.point_hits
	}

	fn point_misses(tier: &RangeTier<D>, id: &TestPartition) -> u64 {
		tier.shard_for(id).lock().metrics.point_misses
	}

	#[test]
	fn the_three_outcomes_are_reached_deliberately_for_one_key() {
		// The three outcomes must stay distinct; collapsing them serves a row for a key that is gone.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);

		assert_eq!(tier.lookup(OP_A, &at), None, "an uncovered key must say nothing");

		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"z"));
		assert_eq!(tier.lookup(OP_A, &at), Some(None), "a covered key with no row is a proven absence");

		seat(&tier, id, &at, Entry::row(row("v")));
		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("v"))), "a resident row must be returned");
	}

	#[test]
	fn a_proven_absence_inside_a_claim_needs_no_entry_at_all() {
		// A proven absence must stop here; the caller must never touch the filter or persistent tier.
		let tier = tier();
		let id = partition(CACHED);
		participate(&tier, id);
		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"z"));

		assert_eq!(tier.lookup(OP_A, &key(CACHED, b"m")), Some(None));
		assert_eq!(point_hits(&tier, &id), 1, "a proven absence is a hit, not a miss");
	}

	#[test]
	fn a_key_outside_every_claim_falls_through_rather_than_answering_absent() {
		// RAM has no opinion here; answering none would hide a row the persistent tier still holds.
		let tier = tier();
		let id = partition(CACHED);
		participate(&tier, id);
		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"c"));

		assert_eq!(tier.lookup(OP_A, &key(CACHED, b"m")), None);
		assert_eq!(point_misses(&tier, &id), 1);
	}

	#[test]
	fn a_resident_row_answers_with_no_claim_standing_over_it() {
		// Residency alone is proof; requiring coverage would send the hit path through the one lock.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		seat(&tier, id, &at, Entry::row(row("v")));

		assert!(tier.coverage().read().set(OP_A).is_none(), "the fixture must claim nothing");
		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("v"))));
	}

	#[test]
	fn a_removal_the_flush_has_not_seen_answers_a_definitive_absence() {
		// The read must stop here; falling through reaches the persistent tier, which still holds it.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		seat(&tier, id, &at, Entry::deleted());

		assert_eq!(tier.lookup(OP_A, &at), Some(None));
		assert_eq!(point_hits(&tier, &id), 1);
	}

	#[test]
	fn a_flushed_absence_answers_a_definitive_absence() {
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		seat(&tier, id, &at, Entry::absent());

		assert_eq!(tier.lookup(OP_A, &at), Some(None));
		assert_eq!(point_hits(&tier, &id), 1);
	}

	#[test]
	fn a_removal_and_a_row_at_the_same_key_never_answer_alike() {
		// One key must yield the row and the other none, or the residency states are indistinguishable.
		let tier = tier();
		let id = partition(CACHED);
		let present = key(CACHED, b"m");
		let removed = key(CACHED, b"n");
		seat(&tier, id, &present, Entry::row(row("v")));
		seat(&tier, id, &removed, Entry::deleted());

		assert_eq!(tier.lookup(OP_A, &present), Some(Some(row("v"))));
		assert_eq!(tier.lookup(OP_A, &removed), Some(None));
	}

	#[test]
	fn a_claim_withdrawn_between_the_two_steps_is_not_a_proven_absence() {
		// A claim withdrawn between the two steps must never be answered as a proven absence.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"z"));

		let withdrawing = tier.clone();
		let outside = key(CACHED, b"zz");
		arm_absence_interlock(move || withdrawing.mark_deleted(OP_A, &outside));

		assert_eq!(
			tier.lookup(OP_A, &at),
			None,
			"a claim was withdrawn mid-read, so this tier can no longer speak for the key"
		);
		assert_eq!(point_misses(&tier, &id), 1);
	}

	#[test]
	fn a_row_materialized_between_the_two_steps_is_answered_rather_than_missed() {
		// The row read must come second, or a write landing in between reads as a proven absence.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"z"));

		let writing = tier.clone();
		let landing = at.clone();
		arm_absence_interlock(move || writing.overwrite(OP_A, landing.clone(), row("v")));

		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("v"))));
	}

	#[test]
	fn an_eviction_between_the_two_steps_is_not_a_proven_absence() {
		// An eviction that shrinks coverage must bump the token, or the key it withdrew still reads absent.
		let per_entry = entry_footprint(&key(CACHED, b"f0"), &Entry::row(row("v")));
		let tier = RangeTier::<D>::new(RangeConfig {
			shard_bytes: Some(ByteSize::from_bytes((PARTITION_OVERHEAD + 2 * per_entry) as u64)),
			shards: 1,
			..RangeConfig::testing()
		})
		.expect("a tier with a byte budget must be constructed");
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		participate(&tier, id);
		claim(&tier, &key(CACHED, b"a"), &key(CACHED, b"z"));
		for index in 0..4 {
			seat(&tier, id, &key(CACHED, format!("f{index}").as_bytes()), Entry::row(row("v")));
		}

		assert!(
			tier.shard_for(&id).lock().budget.over_budget(),
			"the fixture must be over budget, or the eviction is a no-op and this test proves nothing"
		);
		assert_eq!(tier.lookup(OP_A, &at), Some(None), "the claim must answer absent before the eviction");

		let evicting = tier.clone();
		arm_absence_interlock(move || evicting.evict_to_capacity(0));

		assert_eq!(
			tier.lookup(OP_A, &at),
			None,
			"the eviction withdrew the claim mid-read, so this tier can no longer speak for the key"
		);
	}

	#[test]
	fn a_key_too_short_to_name_a_partition_is_declined() {
		// The tier cannot tell which claim it would belong to, so it must never answer for it.
		let tier = tier();
		assert_eq!(tier.lookup(OP_A, &EncodedKey::new(b"short")), None);
	}

	#[test]
	fn a_keyspace_that_is_never_cached_is_declined_even_under_a_claim() {
		// An uncacheable keyspace must fall through whatever RAM happens to hold for it.
		let tier = tier();
		let id = partition(UNCACHED);
		let at = key(UNCACHED, b"m");
		seat(&tier, id, &at, Entry::row(row("v")));
		claim(&tier, &key(UNCACHED, b"a"), &key(UNCACHED, b"z"));

		assert_eq!(tier.lookup(OP_A, &at), None);
	}

	#[test]
	fn an_operator_with_no_coverage_at_all_falls_through() {
		// A tier that has never scanned this operator must not manufacture a proven absence.
		let tier = tier();
		participate(&tier, partition(CACHED));

		assert_eq!(tier.lookup(OP_A, &key(CACHED, b"m")), None);
	}

	#[test]
	fn a_hit_bumps_the_keyspace_counter_of_its_own_keyspace() {
		// A counter charged to the wrong slot makes the per-keyspace shape signal meaningless.
		let tier = tier();
		let id = partition(CACHED);
		let at = key(CACHED, b"m");
		seat(&tier, id, &at, Entry::row(row("v")));

		tier.lookup(OP_A, &at);

		let shard = tier.shard_for(&id).lock();
		assert_eq!(shard.slot_metrics[CACHED.0 as usize].point_hits, 1);
		assert_eq!(shard.slot_metrics[UNCACHED.0 as usize].point_hits, 0);
	}
}
