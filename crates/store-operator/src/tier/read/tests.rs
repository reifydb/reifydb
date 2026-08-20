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

use crate::tier::read::{
	BUCKET_OVERHEAD, BucketId, ENTRY_OVERHEAD, FillInterlock, OperatorReadBufferConfig,
	OperatorReadBufferKeyspaceMetrics, OperatorReadBufferMetrics, OperatorReadBufferTier,
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tier(limit: u64) -> OperatorReadBufferTier {
	OperatorReadBufferTier::new(OperatorReadBufferConfig {
		resident_bytes: Some(ByteSize::from_bytes(limit)),
		shards: 1,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn roomy() -> OperatorReadBufferTier {
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

fn footprint(key: &EncodedKey, row: &Option<EncodedPodRow>) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + row.as_ref().map_or(0, EncodedPodRow::len)
}

#[test]
fn a_remembered_row_is_served_back() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(tier.get(OP_A, &k).is_none(), "a key nothing remembered must report unknown so the read falls through");

	tier.remember(OP_A, k.clone(), Some(row("v")));

	let served = tier.get(OP_A, &k).expect("the tier knows the key it was just told about");
	assert_eq!(body(&served), "v", "the tier must hand back the row it was given, not a stale or empty one");
	assert_eq!(tier.contains(OP_A, &k), Some(true));
}

#[test]
fn a_remembered_absence_is_a_hit_and_differs_from_an_unknown_key() {
	let tier = roomy();
	let known_absent = key(GROUP_A, Keyspace::ACCUMULATOR, b"absent");
	let unknown = key(GROUP_A, Keyspace::ACCUMULATOR, b"unknown");

	tier.remember(OP_A, known_absent.clone(), None);

	assert_eq!(
		tier.get(OP_A, &known_absent),
		Some(None),
		"a remembered absence must be served as a hit; collapsing it to a miss sends every repeat read \
		 back to the store, which is the cost this tier exists to remove"
	);
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
	tier.remember(OP_A, dropped.clone(), Some(row("x")));
	tier.remember(OP_A, sibling.clone(), Some(row("y")));
	assert_eq!(tier.buckets(), 1, "both keys share one bucket, so the fixture actually tests sibling survival");

	tier.invalidate(OP_A, &dropped);

	assert_eq!(tier.get(OP_A, &dropped), None, "an invalidated key must go unknown, never stay as a stale row");
	let served = tier.get(OP_A, &sibling).expect("the sibling in the same bucket must survive");
	assert_eq!(body(&served), "y", "invalidating one key must not disturb another key's row");
	assert_eq!(tier.entries(), 1);
}

#[test]
fn invalidate_operator_spares_other_operators() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"k");
	tier.remember(OP_A, k.clone(), Some(row("a")));
	tier.remember(OP_B, k.clone(), Some(row("b")));
	assert_eq!(tier.buckets(), 2, "the same inner key under two operators must occupy two buckets");

	tier.invalidate_operator(OP_A);

	assert_eq!(tier.get(OP_A, &k), None, "a dropped operator must leave no cached state behind");
	let survivor = tier.get(OP_B, &k).expect("another operator's identical key must survive");
	assert_eq!(body(&survivor), "b", "operator scoping must be by bucket, not by key bytes");
	assert_eq!(tier.buckets(), 1);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "dropping an operator must release exactly its bytes");
}

#[test]
fn bucketing_splits_on_operator_group_and_keyspace() {
	let base = BucketId::of(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"a"))
		.expect("a well formed inner key must yield a bucket");

	assert_eq!(
		BucketId::of(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"b")),
		Some(base),
		"the suffix must not take part in bucketing, or every key becomes its own bucket"
	);
	for (name, other) in [
		("keyspace", BucketId::of(OP_A, &key(GROUP_A, Keyspace::BUFFER, b"a"))),
		("group", BucketId::of(OP_A, &key(GROUP_B, Keyspace::ACCUMULATOR, b"a"))),
		("operator", BucketId::of(OP_B, &key(GROUP_A, Keyspace::ACCUMULATOR, b"a"))),
	] {
		assert_ne!(other, Some(base), "changing the {name} must produce a different bucket");
	}

	assert_eq!(base.operator, OP_A);
	assert_eq!(base.group, GROUP_A, "the group must be decoded, not read as raw keycode bytes");
	assert_eq!(base.keyspace, Keyspace::ACCUMULATOR);
}

#[test]
fn two_keys_of_one_bucket_share_one_bucket() {
	let tier = roomy();
	tier.remember(OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), Some(row("a")));
	tier.remember(OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, b"b"), Some(row("b")));
	assert_eq!(tier.buckets(), 1);
	assert_eq!(tier.entries(), 2);

	tier.remember(OP_A, key(GROUP_A, Keyspace::BUFFER, b"a"), Some(row("c")));
	tier.remember(OP_A, key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), Some(row("d")));
	tier.remember(OP_B, key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), Some(row("e")));
	assert_eq!(tier.buckets(), 4, "keyspace, group and operator must each split the bucket");
	assert_eq!(tier.entries(), 5);
}

#[test]
fn filling_past_the_budget_evicts_whole_buckets_and_releases_their_bytes() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_bucket = (BUCKET_OVERHEAD + footprint(&sample_key, &sample_row)) as u64;
	let tier = tier(per_bucket * 2);

	for group in [GROUP_A, GROUP_B] {
		tier.remember(OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	assert_eq!(tier.buckets(), 2, "two buckets fit exactly, so nothing may be evicted yet");
	assert_eq!(tier.evictions(), 0);
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket * 2);

	tier.remember(OP_A, key(GroupId(12), Keyspace::ACCUMULATOR, b"a"), sample_row.clone());

	assert_eq!(tier.evictions(), 1, "the third bucket must push exactly one victim out, not the whole shard");
	assert_eq!(tier.buckets(), 2);
	assert!(
		tier.resident_bytes().as_bytes() <= per_bucket * 2,
		"eviction must bring used bytes back under the limit, or the budget stops bounding anything"
	);
	assert_eq!(
		tier.resident_bytes(),
		tier.tallied_bytes(),
		"the budget must equal the sum of the surviving buckets, or eviction released the wrong amount"
	);
	assert_eq!(
		tier.get(OP_A, &key(GROUP_A, Keyspace::ACCUMULATOR, b"a")),
		None,
		"the least recently touched bucket must be the victim"
	);
	assert!(
		tier.get(OP_A, &key(GroupId(12), Keyspace::ACCUMULATOR, b"a")).is_some(),
		"the bucket that triggered eviction must not evict itself while older buckets remain"
	);
}

#[test]
fn charge_and_release_balance_across_the_entry_lifecycle() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_bucket = (BUCKET_OVERHEAD + footprint(&sample_key, &sample_row)) as u64;
	let tier = tier(per_bucket * 2);
	let balanced = |stage: &str| {
		assert_eq!(
			tier.resident_bytes(),
			tier.tallied_bytes(),
			"after {stage} the budget and the per-bucket tally must agree; a drift either way is a leak \
			 that silently shrinks or unbounds the cache"
		);
	};

	tier.remember(OP_A, sample_key.clone(), sample_row.clone());
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket);
	balanced("insert");

	tier.invalidate(OP_A, &sample_key);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "invalidating the last entry must release the bucket too");
	balanced("invalidate");

	tier.remember(OP_A, sample_key.clone(), sample_row.clone());
	balanced("re-insert");

	tier.remember(OP_A, sample_key.clone(), Some(row("a much longer row body")));
	balanced("overwrite with a larger row");
	tier.remember(OP_A, sample_key.clone(), sample_row.clone());
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket, "shrinking an entry must release the difference");
	balanced("overwrite with a smaller row");

	for group in [GROUP_B, GroupId(12), GroupId(13)] {
		tier.remember(OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	assert!(tier.evictions() > 0, "the fixture must actually reach eviction, or the stage below proves nothing");
	balanced("evict");

	tier.clear();
	assert_eq!(
		tier.resident_bytes(),
		ByteSize::ZERO,
		"clear must release every byte it charged rather than zero the counter, or a leak stays invisible"
	);
	assert_eq!(tier.buckets(), 0);
	assert_eq!(tier.entries(), 0);
}

#[test]
fn a_long_key_charges_its_heap_bytes() {
	let tier = roomy();
	let short = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let long = key(GROUP_A, Keyspace::ACCUMULATOR, &[7u8; 64]);
	assert_eq!(short.heap_bytes(), 0, "the short fixture must stay inline, or the comparison below is meaningless");
	assert!(long.heap_bytes() > 0, "the long fixture must spill to the heap, or nothing tests heap accounting");

	tier.remember(OP_A, short, Some(row("v")));
	let after_short = tier.resident_bytes().as_bytes();
	tier.remember(OP_A, long.clone(), Some(row("v")));

	assert_eq!(
		tier.resident_bytes().as_bytes() - after_short,
		(ENTRY_OVERHEAD + long.heap_bytes() + 1) as u64,
		"a heap allocated key must be charged for its allocation, or long keys are cached for free"
	);
	tier.clear();
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
}

#[test]
fn repeated_reads_of_one_remembered_key_cost_one_miss() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert_eq!(tier.get(OP_A, &k), None);
	tier.remember(OP_A, k.clone(), Some(row("v")));
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
		assert_eq!(BucketId::of(OP_A, &short), None, "a {} byte key cannot carry a bucket", bytes.len());

		tier.remember(OP_A, short.clone(), Some(row("v")));
		assert_eq!(tier.get(OP_A, &short), None, "a declined key must never be served back");
		assert_eq!(tier.contains(OP_A, &short), None);
		tier.invalidate(OP_A, &short);
	}

	assert_eq!(tier.buckets(), 0, "declining must mean not caching, not caching under a wrong bucket");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a declined key must not be charged");
	assert_eq!(tier.hits(), 0);
	assert_eq!(tier.misses(), 0, "an undecodable key is not attributable to a shard, so it counts as neither");

	let shortest_valid = key(GROUP_A, Keyspace::ACCUMULATOR, b"");
	assert_eq!(shortest_valid.len(), 9, "group plus keyspace with an empty suffix is the shortest valid key");
	assert!(BucketId::of(OP_A, &shortest_valid).is_some(), "the shortest valid key must not be declined");
}

#[test]
fn a_complete_bucket_answers_for_keys_it_never_saw() {
	let tier = roomy();
	let present = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let never_seen = key(GROUP_A, Keyspace::ACCUMULATOR, b"z");
	tier.remember(OP_A, present.clone(), Some(row("v")));
	let bucket = BucketId::of(OP_A, &present).expect("the fixture key must decode");

	assert_eq!(tier.get(OP_A, &never_seen), None, "an incomplete bucket must not claim to know about a gap");

	tier.mark_complete(bucket);
	assert!(tier.is_complete(bucket));
	assert_eq!(
		tier.get(OP_A, &never_seen),
		Some(None),
		"once the whole bucket was loaded a missing entry is a known absence, not an unknown"
	);

	tier.invalidate(OP_A, &never_seen);
	assert!(
		!tier.is_complete(bucket),
		"a write into a complete bucket must drop completeness, or the bucket keeps answering absent for a \
		 key that now exists"
	);
	assert_eq!(tier.get(OP_A, &never_seen), None);
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
		"marking dirty must be scoped to the dropped operator, otherwise one drop aborts every live fill"
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
fn a_published_fill_is_accounted_exactly_like_a_remembered_row() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.remember(OP_A, k.clone(), Some(row("v")));
	let by_remember = tier.resident_bytes();
	tier.clear();
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "the fixture must start the second half from empty");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(tier.finish_fill(OP_A, k.clone(), Some(row("v"))));

	assert_eq!(tier.resident_bytes(), by_remember, "publishing a fill must charge what remembering charges");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes(), "the budget must match the per-bucket tally");
	assert_eq!(tier.buckets(), 1);
	assert_eq!(tier.entries(), 1);
}

#[test]
fn a_published_fill_evicts_to_capacity() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_bucket = (BUCKET_OVERHEAD + footprint(&sample_key, &sample_row)) as u64;
	let tier = tier(per_bucket * 2);

	for group in [GROUP_A, GROUP_B, GroupId(12)] {
		let k = key(group, Keyspace::ACCUMULATOR, b"a");
		assert!(tier.begin_fill(OP_A, &k));
		assert!(tier.finish_fill(OP_A, k, sample_row.clone()));
	}

	assert_eq!(tier.evictions(), 1, "the third published fill must push exactly one victim out");
	assert_eq!(tier.buckets(), 2);
	assert!(tier.resident_bytes().as_bytes() <= per_bucket * 2, "eviction must bring used bytes back under");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_second_fill_of_the_same_key_is_declined_while_one_is_in_flight() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sibling = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");

	assert!(tier.begin_fill(OP_A, &k));
	assert!(!tier.begin_fill(OP_A, &k), "a duplicate fill of the same key must be declined");
	assert!(tier.begin_fill(OP_A, &sibling), "a different key in the same bucket must still be admitted");
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
	let hook: FillInterlock = Box::new(move |tier: &OperatorReadBufferTier, id: BucketId| {
		seen.store(true, Ordering::Relaxed);
		flag.store(tier.shard_for(&id).try_lock().is_some(), Ordering::Relaxed);
	});

	let tier = OperatorReadBufferTier::with_interlock(
		OperatorReadBufferConfig {
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
		"the shard lock was acquirable between clearing the fill marker and publishing the row; a writer \
		 invalidating in that window is silently dropped and the stale row is published over it"
	);
}

#[test]
fn a_tier_without_a_byte_budget_is_not_constructed() {
	assert!(OperatorReadBufferTier::new(OperatorReadBufferConfig {
		resident_bytes: None,
		shards: 16,
	})
	.is_none());
	assert!(OperatorReadBufferTier::new(OperatorReadBufferConfig::default()).is_some());
	assert_eq!(OperatorReadBufferConfig::default().shards, 16);
	assert_eq!(OperatorReadBufferConfig::default().resident_bytes, Some(ByteSize::from_mib(64)));
}

#[test]
fn every_shard_is_reachable_and_reports_its_own_slice_of_the_budget() {
	let tier = OperatorReadBufferTier::new(OperatorReadBufferConfig {
		resident_bytes: Some(ByteSize::from_mib(64)),
		shards: 4,
	})
	.expect("a sharded tier must be constructed");

	let metrics = tier.shard_metrics();
	assert_eq!(metrics.len(), 4);
	for (index, shard) in metrics.iter().enumerate() {
		assert_eq!(shard.shard, index);
		assert_eq!(
			shard.limit,
			ByteSize::from_mib(16),
			"the total budget must be split across shards, or the tier holds shards times the limit"
		);
	}

	for group in 0..64u64 {
		tier.remember(OP_A, key(GroupId(group), Keyspace::ACCUMULATOR, b"a"), Some(row("v")));
	}
	assert_eq!(tier.buckets(), 64);
	assert!(
		tier.shard_metrics().iter().all(|shard| shard.buckets > 0),
		"64 buckets must reach all 4 shards, or the shard hash ignores part of the bucket id"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

fn keyspace_row(tier: &OperatorReadBufferTier, keyspace: Keyspace) -> OperatorReadBufferKeyspaceMetrics {
	tier.keyspace_metrics()
		.into_iter()
		.find(|row| row.keyspace == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
}

#[test]
fn keyspace_counters_are_charged_to_the_keyspace_that_was_read() {
	let tier = roomy();
	let accumulator = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let buffer = key(GROUP_A, Keyspace::BUFFER, b"a");

	assert!(tier.get(OP_A, &accumulator).is_none());
	tier.remember(OP_A, accumulator.clone(), Some(row("v")));
	assert!(tier.get(OP_A, &accumulator).is_some());
	assert!(tier.get(OP_A, &buffer).is_none());

	let reported = tier.keyspace_metrics();
	assert_eq!(
		reported.len(),
		2,
		"only the two keyspaces that were touched may be reported; a fixed 256 slot table must never \
		 surface as 256 rows of zeros"
	);

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.hits, 1);
	assert_eq!(accumulator.counters.misses, 1);
	assert_eq!(accumulator.buckets, 1);
	assert_eq!(accumulator.entries, 1);

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.hits, 0, "a miss in one keyspace must not borrow the other keyspace's hit");
	assert_eq!(buffer.counters.misses, 1);
	assert_eq!(buffer.buckets, 0, "a keyspace with no resident bucket is still reported once a counter moved");

	assert_eq!(tier.metrics().hits, 1, "the per shard aggregate must survive alongside the keyspace table");
	assert_eq!(tier.metrics().misses, 2);
}

#[test]
fn contains_charges_the_same_keyspace_slots_as_get() {
	let tier = roomy();
	let known = key(GROUP_A, Keyspace::EMIT, b"a");
	let unknown = key(GROUP_A, Keyspace::EXPIRY, b"a");

	tier.remember(OP_A, known.clone(), Some(row("v")));
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
	let per_bucket = (BUCKET_OVERHEAD + footprint(&sample_key, &sample_row)) as u64;

	for group in [GROUP_A, GROUP_B] {
		tier.remember(OP_A, key(group, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	}
	let complete = key(GROUP_A, Keyspace::BUFFER, b"a");
	tier.remember(OP_A, complete.clone(), sample_row.clone());
	tier.mark_complete(BucketId::of(OP_A, &complete).expect("the fixture key must decode"));

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.buckets, 2);
	assert_eq!(accumulator.entries, 2);
	assert_eq!(accumulator.complete_buckets, 0);
	assert_eq!(accumulator.used, ByteSize::from_bytes(per_bucket * 2));

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.buckets, 1);
	assert_eq!(buffer.entries, 1);
	assert_eq!(buffer.complete_buckets, 1, "completeness must follow the bucket into its own keyspace row");
	assert_eq!(buffer.used, ByteSize::from_bytes(per_bucket));

	let total: u64 = tier.keyspace_metrics().iter().map(|row| row.used.as_bytes()).sum();
	assert_eq!(
		ByteSize::from_bytes(total),
		tier.tallied_bytes(),
		"every resident byte must be attributed to exactly one keyspace, or the table leaks or double counts"
	);
	assert_eq!(tier.keyspace_metrics().iter().map(|row| row.buckets).sum::<usize>(), tier.buckets());
	assert_eq!(tier.keyspace_metrics().iter().map(|row| row.entries).sum::<usize>(), tier.entries());
}

#[test]
fn keyspace_counters_are_summed_across_every_shard() {
	let tier = OperatorReadBufferTier::new(OperatorReadBufferConfig {
		resident_bytes: Some(ByteSize::from_mib(64)),
		shards: 4,
	})
	.expect("a sharded tier must be constructed");

	for group in 0..64u64 {
		tier.remember(OP_A, key(GroupId(group), Keyspace::SOURCE_WATERMARK, b"a"), Some(row("v")));
	}
	for group in 0..64u64 {
		assert!(tier.get(OP_A, &key(GroupId(group), Keyspace::SOURCE_WATERMARK, b"a")).is_some());
	}

	assert!(
		tier.shard_metrics().iter().filter(|shard| shard.counters.hits > 0).count() > 1,
		"the fixture must spread hits over more than one shard, or summation is not under test"
	);

	let reported = tier.keyspace_metrics();
	assert_eq!(reported.len(), 1, "one keyspace spread over four shards must collapse to a single row");
	assert_eq!(reported[0].keyspace, Keyspace::SOURCE_WATERMARK);
	assert_eq!(reported[0].counters.hits, 64);
	assert_eq!(reported[0].buckets, 64);
	assert_eq!(reported[0].entries, 64);
}

#[test]
fn an_eviction_is_charged_to_the_evicted_bucket_keyspace() {
	let sample_key = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let sample_row = Some(row("v"));
	let per_bucket = (BUCKET_OVERHEAD + footprint(&sample_key, &sample_row)) as u64;
	let tier = tier(per_bucket);

	tier.remember(OP_A, key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), sample_row.clone());
	tier.remember(OP_A, key(GROUP_A, Keyspace::BUFFER, b"a"), sample_row.clone());

	assert_eq!(tier.evictions(), 1, "the fixture must actually evict, or the attribution below proves nothing");

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.evictions, 1);
	assert_eq!(accumulator.buckets, 0, "the evicted bucket must be gone from its keyspace's resident state");

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.evictions, 0, "the survivor must not be charged for the victim's eviction");
	assert_eq!(buffer.buckets, 1);
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
	assert!(tier.keyspace_metrics().is_empty());

	let resident = key(GROUP_A, Keyspace::JOIN_LEFT, b"a");
	tier.remember(OP_A, resident.clone(), Some(row("v")));
	assert_eq!(tier.keyspace_metrics().len(), 1, "resident state alone must be enough to report a keyspace");
	assert_eq!(tier.keyspace_metrics()[0].keyspace, Keyspace::JOIN_LEFT);
	assert_eq!(tier.keyspace_metrics()[0].counters, OperatorReadBufferMetrics::default());

	assert!(tier.get(OP_A, &resident).is_some());
	tier.clear();
	assert_eq!(
		tier.keyspace_metrics().len(),
		1,
		"clearing drops resident state but not the counters, so a keyspace with history stays reported"
	);
	assert_eq!(tier.keyspace_metrics()[0].buckets, 0);
}
