// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::point::{
	ENTRY_OVERHEAD, FillInterlock, PointConfig, PointKey, PointMetrics, PointSlotMetrics, PointTier,
	domain::{ChainingDomain as C, TestDomain as D, keyspace_of},
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn sharded(limit: u64, shards: usize) -> PointTier<D> {
	PointTier::<D>::new(PointConfig {
		resident_bytes: Some(ByteSize::from_bytes(limit)),
		shards,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn tier(limit: u64) -> PointTier<D> {
	sharded(limit, 1)
}

fn roomy() -> PointTier<D> {
	tier(ByteSize::from_mib(1).as_bytes())
}

fn key(group: GroupId, keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(entry: &Option<EncodedPodRow>) -> String {
	String::from_utf8(entry.as_ref().expect("entry must carry a row").body().to_vec())
		.expect("test bodies are utf8")
}

fn fill(tier: &PointTier<D>, operator: OperatorId, key: EncodedKey, row: Option<EncodedPodRow>) {
	assert!(tier.begin_fill(operator, &key), "the fixture must be allowed to start the fill it is staging");
	assert!(tier.finish_fill(operator, key, row), "the fixture must be allowed to publish the fill it started");
}

fn footprint(key: &EncodedKey, row: &Option<EncodedPodRow>) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + row.as_ref().map_or(0, EncodedPodRow::len)
}

fn keyspace_row(tier: &PointTier<D>, keyspace: Keyspace) -> PointSlotMetrics<D> {
	tier.slot_metrics()
		.into_iter()
		.find(|row| row.slot == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
}

#[test]
fn a_remembered_row_is_served_back() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.get(OP_A, &k).is_none(), "a key nothing remembered must report unknown so the read falls through");

	assert!(tier.begin_fill(OP_A, &k), "the miss above must be allowed to start the fill that answers it");
	assert!(tier.finish_fill(OP_A, k.clone(), Some(row("v"))), "an undisturbed fill must publish its row");

	let served = tier.get(OP_A, &k).expect("the tier knows the key it was just told about");
	assert_eq!(body(&served), "v", "the tier must hand back the row it was given, not a stale or empty one");
	assert_eq!(tier.contains(OP_A, &k), Some(true));
}

#[test]
fn a_remembered_absence_is_a_hit_and_differs_from_an_unknown_key() {
	let tier = roomy();
	let known_absent = key(GROUP_A, Keyspace::ACCUMULATOR, b"absent");
	let unknown = key(GROUP_A, Keyspace::ACCUMULATOR, b"unknown");

	assert!(tier.begin_fill(OP_A, &known_absent));
	assert!(
		tier.finish_fill(OP_A, known_absent.clone(), None),
		"a fill that found nothing must publish the absence"
	);

	assert_eq!(tier.get(OP_A, &known_absent), Some(None), "a remembered absence must be served as a hit");
	assert_eq!(tier.contains(OP_A, &known_absent), Some(false));
	assert_eq!(tier.hits(), 2, "both the get and the contains of a known absence are hits");

	assert_eq!(tier.get(OP_A, &unknown), None, "an unknown key must stay distinguishable from a known absence");
	assert_eq!(tier.contains(OP_A, &unknown), None);
	assert_eq!(tier.misses(), 2);
}

#[test]
fn invalidate_drops_only_the_named_key() {
	let tier = roomy();
	let dropped = key(GROUP_A, Keyspace::ACCUMULATOR, b"x");
	let sibling = key(GROUP_A, Keyspace::ACCUMULATOR, b"y");
	tier.overwrite(OP_A, dropped.clone(), row("x"));
	tier.overwrite(OP_A, sibling.clone(), row("y"));
	assert_eq!(tier.entries(), 2, "both keys must be resident, or sibling survival is not under test");

	tier.invalidate(OP_A, &dropped);

	assert_eq!(tier.get(OP_A, &dropped), None, "an invalidated key must go unknown, never stay as a stale row");
	let served = tier.get(OP_A, &sibling).expect("the sibling sharing the group and keyspace must survive");
	assert_eq!(body(&served), "y", "invalidating one key must not disturb another key's row");
	assert_eq!(tier.entries(), 1);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the drop must release exactly the dropped bytes");
}

#[test]
fn invalidate_operator_spares_other_operators() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"k");
	tier.overwrite(OP_A, k.clone(), row("a"));
	tier.overwrite(OP_B, k.clone(), row("b"));
	assert_eq!(tier.entries(), 2, "the same inner key under two operators must occupy two entries");

	tier.invalidate_operator(OP_A);

	assert_eq!(tier.get(OP_A, &k), None, "a dropped operator must leave no cached state behind");
	let survivor = tier.get(OP_B, &k).expect("another operator's identical key must survive");
	assert_eq!(body(&survivor), "b", "operator scoping must be by entry, not by key bytes");
	assert_eq!(tier.entries(), 1);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "dropping an operator must release exactly its bytes");
	assert!(tier.index_is_consistent(), "the rebuilt index must address the surviving slots");
}

#[test]
fn filling_past_the_budget_evicts_entries_and_releases_their_bytes() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry * 2);

	for group in [GROUP_A, GROUP_B] {
		fill(&tier, OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	assert_eq!(tier.entries(), 2, "two entries fit exactly, so nothing may be evicted yet");
	assert_eq!(tier.evictions(), 0);
	assert_eq!(tier.resident_bytes().as_bytes(), per_entry * 2);

	fill(&tier, OP_A, key(GroupId(12), Keyspace::ACCUMULATOR, b"a"), sample_row.clone());

	assert_eq!(tier.evictions(), 1, "the third entry must push exactly one victim out, not the whole shard");
	assert_eq!(tier.entries(), 2);
	assert!(
		tier.resident_bytes().as_bytes() <= per_entry * 2,
		"eviction must bring used bytes back under the limit, or the budget stops bounding anything"
	);
	assert_eq!(
		tier.resident_bytes(),
		tier.tallied_bytes(),
		"the budget must equal the sum of the surviving entries, or eviction released the wrong amount"
	);
	assert_eq!(
		tier.get(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"a")),
		None,
		"the least recently touched entry must be the victim"
	);
	assert!(
		tier.get(OP_A, &key(GroupId(12), Keyspace::ACCUMULATOR, b"a")).is_some(),
		"the entry that triggered eviction must not evict itself while older entries remain"
	);
}

#[test]
fn charge_and_release_balance_across_the_entry_lifecycle() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry * 2);
	let balanced = |stage: &str| {
		assert_eq!(
			tier.resident_bytes(),
			tier.tallied_bytes(),
			"after {stage} the budget and the per-entry tally must agree"
		);
	};

	fill(&tier, OP_A, sample_key.clone(), sample_row.clone());
	assert_eq!(tier.resident_bytes().as_bytes(), per_entry);
	balanced("insert");

	tier.invalidate(OP_A, &sample_key);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "invalidating the last entry must release its bytes");
	balanced("invalidate");

	fill(&tier, OP_A, sample_key.clone(), sample_row.clone());
	balanced("re-insert");

	fill(&tier, OP_A, sample_key.clone(), Some(row("a much longer row body")));
	balanced("overwrite with a larger row");
	fill(&tier, OP_A, sample_key.clone(), sample_row.clone());
	assert_eq!(tier.resident_bytes().as_bytes(), per_entry, "shrinking an entry must release the difference");
	balanced("overwrite with a smaller row");

	for group in [GROUP_B, GroupId(12), GroupId(13)] {
		fill(&tier, OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	assert!(tier.evictions() > 0, "the fixture must actually reach eviction, or the stage below proves nothing");
	balanced("evict");

	tier.clear();
	assert_eq!(
		tier.resident_bytes(),
		ByteSize::ZERO,
		"clear must release every byte it charged rather than zero the counter, or a leak stays invisible"
	);
	assert_eq!(tier.entries(), 0);
}

#[test]
fn a_long_key_charges_its_heap_bytes() {
	let tier = roomy();
	let short = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let long = key(GROUP_A, Keyspace::ACCUMULATOR, &[7u8; 64]);
	assert_eq!(short.heap_bytes(), 0, "the short fixture must stay inline, or the comparison below is meaningless");
	assert!(long.heap_bytes() > 0, "the long fixture must spill to the heap, or nothing tests heap accounting");

	tier.overwrite(OP_A, short, row("v"));
	let after_short = tier.resident_bytes().as_bytes();
	tier.overwrite(OP_A, long.clone(), row("v"));

	assert_eq!(
		tier.resident_bytes().as_bytes() - after_short,
		(ENTRY_OVERHEAD + long.heap_bytes() + 1) as u64,
		"a heap allocated key must be charged for its allocation"
	);
	tier.clear();
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
}

#[test]
fn repeated_reads_of_one_remembered_key_cost_one_miss() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert_eq!(tier.get(OP_A, &k), None);
	fill(&tier, OP_A, k.clone(), Some(row("v")));
	for _ in 0..16 {
		assert!(tier.get(OP_A, &k).is_some(), "every read after the first must be served from the tier");
	}

	assert_eq!(tier.misses(), 1, "many reads of one immutable row must cost exactly one store read");
	assert_eq!(tier.hits(), 16);
}

#[test]
fn a_key_too_short_to_carry_a_keyspace_is_declined_not_cached() {
	let tier = roomy();

	for bytes in [vec![], vec![0u8], vec![0u8; 8]] {
		let short = EncodedKey::new(&bytes);
		assert_eq!(keyspace_of(&short), None, "a {} byte key cannot carry a keyspace", bytes.len());

		assert!(!tier.begin_fill(OP_A, &short), "a key with no keyspace cannot be filled");
		assert!(!tier.finish_fill(OP_A, short.clone(), Some(row("v"))), "nor published through the handshake");
		tier.overwrite(OP_A, short.clone(), row("v"));
		assert_eq!(tier.get(OP_A, &short), None, "a declined key must never be served back");
		assert_eq!(tier.contains(OP_A, &short), None);
		tier.invalidate(OP_A, &short);
	}

	assert_eq!(tier.entries(), 0, "declining must mean not caching, not caching under a wrong keyspace");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a declined key must not be charged");
	assert_eq!(tier.hits(), 0);
	assert_eq!(tier.misses(), 0, "an undecodable key is not attributable to a keyspace, so it counts as neither");

	let shortest_valid = key(GROUP_A, Keyspace::ACCUMULATOR, b"");
	assert_eq!(shortest_valid.len(), 9, "group plus keyspace with an empty suffix is the shortest valid key");
	assert!(keyspace_of(&shortest_valid).is_some(), "the shortest valid key must not be declined");
}

#[test]
fn a_fill_invalidated_while_in_flight_is_discarded() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.begin_fill(OP_A, &k), "a first fill of an idle key must be admitted");
	tier.invalidate(OP_A, &k);

	assert!(!tier.finish_fill(OP_A, k.clone(), Some(row("stale"))), "a dirtied fill must report that it discarded");
	assert_eq!(tier.get(OP_A, &k), None, "the discarded value must leave the key unknown, never cached");
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.metrics().fills_dirty_aborted, 1);
}

#[test]
fn a_fill_dirtied_by_an_operator_drop_is_discarded() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let neighbour = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(tier.begin_fill(OP_B, &neighbour));
	tier.invalidate_operator(OP_A);

	assert!(!tier.finish_fill(OP_A, k.clone(), Some(row("stale"))));
	assert_eq!(tier.get(OP_A, &k), None);
	assert!(
		tier.finish_fill(OP_B, neighbour.clone(), Some(row("fresh"))),
		"marking dirty must be scoped to the dropped operator"
	);
	assert!(tier.get(OP_B, &neighbour).is_some());
}

#[test]
fn an_undisturbed_fill_populates_the_tier() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(tier.finish_fill(OP_A, k.clone(), Some(row("v"))), "a clean fill must report that it populated");

	let served = tier.get(OP_A, &k).expect("a clean fill must be readable back");
	assert_eq!(body(&served), "v");
	assert_eq!(tier.metrics().fills_started, 1);
	assert_eq!(tier.metrics().fills_dirty_aborted, 0);
	assert!(tier.begin_fill(OP_A, &k), "finishing a fill must release the slot, or the key can never refill");
	tier.abort_fill(OP_A, &k);
	assert!(tier.begin_fill(OP_A, &k), "aborting must release the slot too");
}

#[test]
fn a_published_fill_is_accounted_exactly_like_an_overwritten_row() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("v"));
	let by_overwrite = tier.resident_bytes();
	tier.clear();
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "the fixture must start the second half from empty");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(tier.finish_fill(OP_A, k.clone(), Some(row("v"))));

	assert_eq!(tier.resident_bytes(), by_overwrite, "publishing a fill must charge what an overwrite charges");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the budget must match the per-entry tally");
	assert_eq!(tier.entries(), 1);
}

#[test]
fn a_published_fill_evicts_to_capacity() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry * 2);

	for group in [GROUP_A, GROUP_B, GroupId(12)] {
		let k = key(group, Keyspace::ACCUMULATOR, b"a");
		assert!(tier.begin_fill(OP_A, &k));
		assert!(tier.finish_fill(OP_A, k, sample_row.clone()));
	}

	assert_eq!(tier.evictions(), 1, "the third published fill must push exactly one victim out");
	assert_eq!(tier.entries(), 2);
	assert!(tier.resident_bytes().as_bytes() <= per_entry * 2, "eviction must bring used bytes back under");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_second_fill_of_the_same_key_is_declined_while_one_is_in_flight() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sibling = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(!tier.begin_fill(OP_A, &k), "a duplicate fill of the same key must be declined");
	assert!(tier.begin_fill(OP_A, &sibling), "a different key in the same keyspace must still be admitted");
	assert!(tier.begin_fill(OP_B, &k), "the same inner key under another operator is a different fill");

	assert_eq!(tier.metrics().fills_duplicate, 1);
	assert_eq!(tier.metrics().fills_started, 3);
}

#[test]
fn clearing_the_tier_discards_every_fill_in_flight() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.begin_fill(OP_A, &k));
	tier.clear();

	assert!(!tier.finish_fill(OP_A, k.clone(), Some(row("stale"))), "a fill whose slot vanished must discard");
	assert_eq!(tier.get(OP_A, &k), None);
	assert_eq!(tier.entries(), 0);
}

#[test]
fn finish_fill_publishes_under_the_lock_that_cleared_the_marker() {
	let acquired = Arc::new(AtomicBool::new(false));
	let probed = Arc::new(AtomicBool::new(false));
	let flag = acquired.clone();
	let seen = probed.clone();
	let hook: FillInterlock<D> = Box::new(move |tier: &PointTier<D>, id: &PointKey<OperatorId>| {
		seen.store(true, Ordering::Relaxed);
		flag.store(tier.shard_for(id).try_lock().is_some(), Ordering::Relaxed);
	});

	let tier = PointTier::<D>::with_interlock(
		PointConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(tier.finish_fill(OP_A, k.clone(), Some(row("v"))), "an undirtied fill must publish");

	assert!(probed.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert!(
		!acquired.load(Ordering::Relaxed),
		"the shard lock was acquirable between clearing the fill marker and publishing the row"
	);
}

#[test]
fn a_tier_without_a_byte_budget_is_not_constructed() {
	assert!(PointTier::<D>::new(PointConfig {
		resident_bytes: None,
		shards: 16,
	})
	.is_none());
	assert!(PointTier::<D>::new(PointConfig::default()).is_some());
	assert_eq!(PointConfig::default().shards, 16);
	assert_eq!(PointConfig::default().resident_bytes, Some(ByteSize::from_mib(64)));
}

#[test]
fn every_shard_is_reachable_and_reports_its_own_slice_of_the_budget() {
	let tier = sharded(ByteSize::from_mib(64).as_bytes(), 4);

	let metrics = tier.shard_metrics();
	assert_eq!(metrics.len(), 4);
	for (index, shard) in metrics.iter().enumerate() {
		assert_eq!(shard.shard, index);
		assert_eq!(shard.limit, ByteSize::from_mib(16), "the total budget must be split across shards");
	}

	for group in 0..64u64 {
		tier.overwrite(OP_A, key(GroupId(group), Keyspace::ACCUMULATOR, b"a"), row("v"));
	}
	assert_eq!(tier.entries(), 64);
	assert!(
		tier.shard_metrics().iter().all(|shard| shard.entries > 0),
		"64 entries must reach all 4 shards, or the shard hash ignores part of the key"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn keyspace_counters_are_charged_to_the_keyspace_that_was_read() {
	let tier = roomy();
	let accumulator = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let buffer = key(GROUP_A, Keyspace::BUFFER, b"a");

	assert!(tier.get(OP_A, &accumulator).is_none());
	fill(&tier, OP_A, accumulator.clone(), Some(row("v")));
	assert!(tier.get(OP_A, &accumulator).is_some());
	assert!(tier.get(OP_A, &buffer).is_none());

	let reported = tier.slot_metrics();
	assert_eq!(reported.len(), 2, "only the two keyspaces that were touched may be reported");

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.hits, 1);
	assert_eq!(accumulator.counters.misses, 1);
	assert_eq!(accumulator.entries, 1);

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.hits, 0, "a miss in one keyspace must not borrow the other keyspace's hit");
	assert_eq!(buffer.counters.misses, 1);
	assert_eq!(buffer.entries, 0, "a keyspace with no resident entry is still reported once a counter moved");

	assert_eq!(tier.metrics().hits, 1, "the per shard aggregate must survive alongside the keyspace table");
	assert_eq!(tier.metrics().misses, 2);
}

#[test]
fn contains_charges_the_same_keyspace_slots_as_get() {
	let tier = roomy();
	let known = key(GROUP_A, Keyspace::EMIT, b"a");
	let unknown = key(GROUP_A, Keyspace::EXPIRY, b"a");

	tier.overwrite(OP_A, known.clone(), row("v"));
	assert_eq!(tier.contains(OP_A, &known), Some(true));
	assert_eq!(tier.contains(OP_A, &unknown), None);

	assert_eq!(keyspace_row(&tier, Keyspace::EMIT).counters.hits, 1);
	assert_eq!(keyspace_row(&tier, Keyspace::EMIT).counters.misses, 0);
	assert_eq!(keyspace_row(&tier, Keyspace::EXPIRY).counters.misses, 1);
	assert_eq!(keyspace_row(&tier, Keyspace::EXPIRY).counters.hits, 0);
}

#[test]
fn resident_state_is_grouped_by_keyspace_and_sums_to_the_tier_total() {
	let tier = roomy();
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;

	for group in [GROUP_A, GROUP_B] {
		fill(&tier, OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	let buffer_key = key(GROUP_A, Keyspace::BUFFER, b"a");
	fill(&tier, OP_A, buffer_key.clone(), sample_row.clone());

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.entries, 2);
	assert_eq!(accumulator.used, ByteSize::from_bytes(per_entry * 2));

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.entries, 1);
	assert_eq!(buffer.used, ByteSize::from_bytes(per_entry));

	let total: u64 = tier.slot_metrics().iter().map(|row| row.used.as_bytes()).sum();
	assert_eq!(ByteSize::from_bytes(total), tier.tallied_bytes(), "every resident byte belongs to one keyspace");
	assert_eq!(tier.slot_metrics().iter().map(|row| row.entries).sum::<usize>(), tier.entries());
}

#[test]
fn keyspace_counters_are_summed_across_every_shard() {
	let tier = sharded(ByteSize::from_mib(64).as_bytes(), 4);

	for group in 0..64u64 {
		tier.overwrite(OP_A, key(GroupId(group), Keyspace::SOURCE_WATERMARK, b"a"), row("v"));
	}
	for group in 0..64u64 {
		assert!(tier.get(OP_A, &key(GroupId(group), Keyspace::SOURCE_WATERMARK, b"a")).is_some());
	}

	assert!(
		tier.shard_metrics().iter().filter(|shard| shard.counters.hits > 0).count() > 1,
		"the fixture must spread hits over more than one shard, or summation is not under test"
	);

	let reported = tier.slot_metrics();
	assert_eq!(reported.len(), 1, "one keyspace spread over four shards must collapse to a single row");
	assert_eq!(reported[0].slot, Keyspace::SOURCE_WATERMARK);
	assert_eq!(reported[0].counters.hits, 64);
	assert_eq!(reported[0].entries, 64);
}

#[test]
fn an_eviction_is_charged_to_the_evicted_entry_keyspace() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry);

	fill(&tier, OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	fill(&tier, OP_A, key(GROUP_A, Keyspace::BUFFER, b"a"), sample_row.clone());

	assert_eq!(tier.evictions(), 1, "the fixture must actually evict, or the attribution below proves nothing");

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.evictions, 1);
	assert_eq!(accumulator.entries, 0, "the evicted entry must be gone from its keyspace's resident state");

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.evictions, 0, "the survivor must not be charged for the victim's eviction");
	assert_eq!(buffer.entries, 1);
}

#[test]
fn fill_counters_are_charged_to_the_filled_keyspace() {
	let tier = roomy();
	let accumulator = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let buffer = key(GROUP_A, Keyspace::BUFFER, b"a");

	assert!(tier.begin_fill(OP_A, &accumulator));
	assert!(!tier.begin_fill(OP_A, &accumulator), "a second fill of the same key must be declined as duplicate");
	assert!(tier.begin_fill(OP_A, &buffer));
	tier.invalidate(OP_A, &accumulator);
	assert!(!tier.finish_fill(OP_A, accumulator.clone(), Some(row("v"))), "a dirtied fill must not publish");

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.fills_started, 1);
	assert_eq!(accumulator.counters.fills_duplicate, 1);
	assert_eq!(accumulator.counters.fills_dirty_aborted, 1);

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.fills_started, 1);
	assert_eq!(buffer.counters.fills_duplicate, 0, "the duplicate belongs to the keyspace that was refilled");
	assert_eq!(buffer.counters.fills_dirty_aborted, 0);

	assert_eq!(tier.metrics().fills_started, 2, "the shard aggregate must still count every fill");
}

#[test]
fn a_tier_that_was_never_read_reports_no_keyspace_rows() {
	let tier = roomy();
	assert!(tier.slot_metrics().is_empty(), "an untouched tier must not surface its 256 empty slots");

	let resident = key(GROUP_A, Keyspace::JOIN_LEFT, b"a");
	let charged = footprint(&resident, &Some(row("v"))) as u64;
	tier.overwrite(OP_A, resident.clone(), row("v"));
	assert_eq!(tier.slot_metrics().len(), 1, "the keyspace that was written must be the only one reported");
	assert_eq!(tier.slot_metrics()[0].slot, Keyspace::JOIN_LEFT);
	assert_eq!(
		tier.slot_metrics()[0].entries,
		1,
		"the row must carry the keyspace's residency, not only its counters"
	);
	assert_eq!(tier.slot_metrics()[0].used, ByteSize::from_bytes(charged));
	assert_eq!(tier.slot_metrics()[0].counters.hits, 0);
	assert_eq!(tier.slot_metrics()[0].counters.misses, 0);

	assert!(tier.get(OP_A, &resident).is_some());
	tier.clear();
	assert_eq!(tier.slot_metrics().len(), 1, "clearing drops resident state but not the counters");
	assert_eq!(tier.slot_metrics()[0].entries, 0, "and the residency it reports must go with the state");
	assert_eq!(tier.slot_metrics()[0].counters.hits, 1, "the hit taken before the clear must survive it");
}

#[test]
fn an_overwrite_publishes_the_row_instead_of_dropping_the_entry() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::NODE_COUNTER, b"a");

	tier.overwrite(OP_A, k.clone(), row("v1"));
	assert_eq!(body(&tier.get(OP_A, &k).expect("an overwritten key must be known")), "v1");

	tier.overwrite(OP_A, k.clone(), row("v2"));
	assert_eq!(body(&tier.get(OP_A, &k).expect("the key stays known across overwrites")), "v2");
	assert_eq!(tier.metrics().misses, 0, "every read here was answerable from the tier");
	assert_eq!(tier.entries(), 1, "an overwrite must reuse the entry, not add a second one for the same key");
}

#[test]
fn an_overwrite_dirties_an_in_flight_fill_so_the_stale_row_cannot_publish() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::NODE_COUNTER, b"a");

	assert!(tier.begin_fill(OP_A, &k), "the fill must start before the overwrite for this to prove anything");
	tier.overwrite(OP_A, k.clone(), row("flushed"));

	assert!(
		!tier.finish_fill(OP_A, k.clone(), Some(row("pre-flush"))),
		"a fill racing an overwrite must be refused, not published"
	);
	assert_eq!(
		body(&tier.get(OP_A, &k).expect("the overwritten row survives the refused fill")),
		"flushed",
		"the refused fill overwrote the newer row"
	);
	assert_eq!(tier.metrics().fills_dirty_aborted, 1);
}

const EXCLUDED: [Keyspace; 5] = [
	Keyspace::CUSTOM_NOT_CACHED,
	Keyspace::JOIN_PIN,
	Keyspace::ENGINE_META,
	Keyspace::EXPIRY,
	Keyspace::TIMER_WHEEL,
];

#[test]
fn no_admission_path_lets_an_excluded_keyspace_into_the_tier() {
	let tier = roomy();

	for keyspace in EXCLUDED {
		let present = key(GROUP_A, keyspace, b"a");
		let absent = key(GROUP_A, keyspace, b"b");

		assert!(!tier.begin_fill(OP_A, &present), "{} must be refused before the fill starts", keyspace.name());
		assert!(
			!tier.finish_fill(OP_A, present.clone(), Some(row("v"))),
			"a fill that was never admitted must not publish {} through the back of the handshake",
			keyspace.name()
		);
		assert!(
			!tier.finish_fill(OP_A, absent.clone(), None),
			"an absence costs the same entry overhead as a row, so {} must be refused on that path too",
			keyspace.name()
		);
		tier.overwrite(OP_A, present.clone(), row("v"));

		assert_eq!(
			tier.get(OP_A, &present),
			None,
			"{} must stay unknown so the read falls through to the store",
			keyspace.name()
		);
		assert_eq!(tier.get(OP_A, &absent), None, "{} must remember no absence either", keyspace.name());
	}

	assert_eq!(tier.entries(), 0, "an excluded keyspace must leave no entry behind");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "an excluded keyspace must not be charged a byte");
	assert_eq!(tier.metrics().fills_started, 0, "a refused keyspace must not even be counted as filled");

	let cached = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	tier.overwrite(OP_A, cached.clone(), row("v"));
	assert!(
		tier.get(OP_A, &cached).is_some(),
		"the control: a gate that refused every keyspace would pass the assertions above while turning the \
		 whole tier into an off switch"
	);
}

#[test]
fn a_point_read_of_an_excluded_keyspace_still_charges_its_miss() {
	let tier = roomy();

	for keyspace in EXCLUDED {
		let k = key(GROUP_A, keyspace, b"a");
		assert_eq!(tier.get(OP_A, &k), None);
		assert_eq!(tier.contains(OP_A, &k), None);
	}

	for keyspace in EXCLUDED {
		let reported = keyspace_row(&tier, keyspace);
		assert_eq!(
			reported.counters.misses,
			2,
			"{} must charge a miss for the get and a miss for the contains",
			keyspace.name()
		);
		assert_eq!(
			reported.counters.hits,
			0,
			"{} holds no entry, so a hit here would mean the exclusion stopped holding",
			keyspace.name()
		);
		assert_eq!(reported.entries, 0, "{} must own no entry", keyspace.name());
	}

	assert_eq!(
		tier.misses(),
		2 * EXCLUDED.len() as u64,
		"the tier aggregate must count these reads too, or a refused keyspace read is missing from the only \
		 counter a replay actually watches"
	);
	assert_eq!(tier.hits(), 0);
	assert_eq!(tier.entries(), 0, "reading an excluded keyspace must not be a back door into admitting it");
}

#[test]
fn one_keyspaces_entries_spread_across_shards() {
	let tier = sharded(ByteSize::from_mib(1).as_bytes(), 16);
	for index in 0..512 {
		let k = key(GroupId::ROOT, Keyspace::ACCUMULATOR, format!("g{index}").as_bytes());
		tier.overwrite(OP_A, k, row("v"));
	}

	assert_eq!(tier.entries(), 512, "the fixture must fit without eviction, or the spread is not what is measured");
	assert!(
		tier.occupied_shards() > 8,
		"one group's keys in one keyspace must land on many shards, not collapse onto a single one"
	);
	let busiest = tier.shard_metrics().iter().map(|shard| shard.entries).max().expect("shards must be reported");
	assert!(busiest < 512, "no single shard may hold every key, or the whole tier serialises behind one mutex");
}

#[test]
fn eviction_removes_one_entry_not_every_key_sharing_a_group() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"k0");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry * 3);

	for index in 0..4 {
		let k = key(GROUP_A, Keyspace::ACCUMULATOR, format!("k{index}").as_bytes());
		fill(&tier, OP_A, k, sample_row.clone());
	}

	assert_eq!(tier.evictions(), 1, "exactly one entry may go, not every key that shared the group");
	assert_eq!(tier.entries(), 3, "three of the four keys must survive their neighbour's eviction");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());

	let survivors = (0..4)
		.filter(|index| {
			tier.get(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, format!("k{index}").as_bytes())).is_some()
		})
		.count();
	assert_eq!(survivors, 3, "every surviving entry must still be readable, not just counted");
}

#[test]
fn the_index_stays_consistent_with_the_slab() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"k0");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let tier = tier(per_entry * 4);

	for index in 0..4 {
		fill(
			&tier,
			OP_A,
			key(GROUP_A, Keyspace::ACCUMULATOR, format!("k{index}").as_bytes()),
			sample_row.clone(),
		);
	}
	assert!(tier.index_is_consistent(), "insert must leave every index position addressing its own slot");

	tier.overwrite(OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, b"k1"), row("replaced"));
	assert!(tier.index_is_consistent(), "an overwrite must reuse the position, not orphan it");

	tier.invalidate(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"k0"));
	assert!(tier.index_is_consistent(), "a removal must repair the position of the slot swapped into the hole");
	assert_eq!(
		body(&tier.get(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"k1")).expect("k1 must survive")),
		"replaced",
		"the swapped slot must still answer under its own key"
	);

	for index in 4..12 {
		fill(
			&tier,
			OP_A,
			key(GROUP_A, Keyspace::ACCUMULATOR, format!("k{index}").as_bytes()),
			sample_row.clone(),
		);
	}
	assert!(tier.evictions() > 0, "the fixture must actually evict, or the check below repeats the insert case");
	assert!(tier.index_is_consistent(), "eviction must repair the index the same way an invalidate does");

	tier.overwrite(OP_B, key(GROUP_A, Keyspace::ACCUMULATOR, b"other"), row("b"));
	tier.invalidate_operator(OP_A);
	assert!(tier.index_is_consistent(), "the index rebuilt after an operator drop must address the survivors");
	assert_eq!(tier.entries(), 1, "only the other operator's entry may remain");
}

#[test]
fn an_excluded_keyspace_read_acquires_no_shard() {
	let tier = sharded(ByteSize::from_mib(1).as_bytes(), 4);
	let k = key(GROUP_A, Keyspace::TIMER_WHEEL, b"a");

	for _ in 0..32 {
		assert_eq!(tier.get(OP_A, &k), None);
	}

	for shard in tier.shard_metrics() {
		assert_eq!(
			shard.counters,
			PointMetrics::default(),
			"shard {} recorded a counter for a read that must never have reached it",
			shard.shard
		);
		assert_eq!(shard.used, ByteSize::ZERO, "shard {} charged bytes for a refused keyspace", shard.shard);
		assert_eq!(shard.entries, 0);
	}
	assert_eq!(
		keyspace_row(&tier, Keyspace::TIMER_WHEEL).counters.misses,
		32,
		"the miss must still be charged to the keyspace, or the read is invisible"
	);
	assert_eq!(tier.misses(), 32, "and the tier aggregate must fold the lock free counter in");
}

#[test]
fn a_refused_supersede_leaves_the_row_and_its_accounting_alone() {
	let tier = PointTier::<C>::new(PointConfig {
		resident_bytes: Some(ByteSize::from_mib(1)),
		shards: 1,
	})
	.expect("a tier with a byte budget must be constructed");
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("resident"));
	let charged = tier.resident_bytes();
	let entries = tier.entries();

	tier.overwrite(OP_A, k.clone(), row("no"));

	let served = tier.get(OP_A, &k).expect("a refusal must not evict the key it declined to replace");
	assert_eq!(body(&served), "resident", "the refused row replaced the resident one, so a downgrade won");
	assert_eq!(tier.entries(), entries, "the refusal seated a second entry for a key that already had one");
	assert_eq!(charged, tier.resident_bytes(), "the refusal moved the budget for a row it never took");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the budget drifted from the bytes the tier holds");
}

#[test]
fn an_accepted_supersede_recharges_from_the_merged_row() {
	let tier = PointTier::<C>::new(PointConfig {
		resident_bytes: Some(ByteSize::from_mib(1)),
		shards: 1,
	})
	.expect("a tier with a byte budget must be constructed");
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("ab"));
	tier.overwrite(OP_A, k.clone(), row("abcdefgh"));

	let served = tier.get(OP_A, &k).expect("an accepted supersede must leave the key resident");
	assert_eq!(body(&served), "abcdefghab", "the merge dropped what the domain chose to keep");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the budget drifted from the bytes the tier holds");
}

#[test]
fn a_key_read_every_round_survives_eviction_past_the_sample_threshold() {
	// Above EVICTION_SAMPLE the victim comes from a random sample, not a full walk, so a hot key can
	// only be lost if every sampled slot lands on it; a policy that ignores the tick loses it at once.
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"k0");
	let sample_row = Some(row("v"));
	let per_entry = footprint(&sample_key, &sample_row) as u64;
	let resident = 40;
	let tier = tier(per_entry * resident);

	let hot = key(GROUP_A, Keyspace::ACCUMULATOR, b"hot");
	fill(&tier, OP_A, hot.clone(), sample_row.clone());
	for index in 0..resident - 1 {
		fill(&tier, OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, format!("c{index}").as_bytes()), sample_row.clone());
	}
	assert_eq!(tier.entries(), resident as usize, "the fixture must fill the budget exactly before it evicts");
	assert_eq!(tier.evictions(), 0);

	for index in 0..200 {
		assert!(tier.get(OP_A, &hot).is_some(), "the hot key must be readable on round {index}");
		fill(&tier, OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, format!("n{index}").as_bytes()), sample_row.clone());
	}

	assert!(
		tier.evictions() >= 200,
		"each round past the budget must evict, or the fixture never exercised the sampled branch"
	);
	assert!(
		tier.get(OP_A, &hot).is_some(),
		"the key touched every round is the hottest in the shard and must outlive 200 evictions"
	);
	assert_eq!(tier.entries(), resident as usize, "the budget must hold the resident count flat across the run");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the budget drifted from the bytes the tier holds");
}
