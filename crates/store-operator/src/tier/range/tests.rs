// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{
		GroupId, Keyspace, OperatorStateKey, group_data_inner_range, group_identity_inner_range,
		group_inner_range, keyspace_inner_range, keyspace_inner_range_upto,
	},
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::{
	BUCKET_OVERHEAD, BucketId, ENTRY_OVERHEAD, FillInterlock, OperatorRangeConfig, OperatorRangeKeyspaceMetrics,
	OperatorRangeMetrics, OperatorRangeTier, range::bucket_scope,
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tier(limit: u64) -> OperatorRangeTier {
	OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_bytes(limit)),
		shards: 1,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn roomy() -> OperatorRangeTier {
	tier(ByteSize::from_mib(1).as_bytes())
}

fn key(group: GroupId, keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn footprint(key: &EncodedKey, row: &EncodedPodRow) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + row.len()
}

fn bodies(page: &[(EncodedKey, EncodedPodRow)]) -> Vec<String> {
	page.iter().map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")).collect()
}

fn fill(
	tier: &OperatorRangeTier,
	operator: OperatorId,
	group: GroupId,
	keyspace: Keyspace,
	page: &[(EncodedKey, EncodedPodRow)],
) -> BucketId {
	let range = keyspace_inner_range(group, keyspace);
	let bucket = tier.begin_fill(operator, &range).expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, page), "the fixture page must fit the fill it is staging");
	assert!(tier.finish_fill(bucket), "an undisturbed fill must publish the bucket it scanned");
	bucket
}

fn one_row_bucket(tier: &OperatorRangeTier, operator: OperatorId, group: GroupId, keyspace: Keyspace) -> EncodedKey {
	let k = key(group, keyspace, b"a");
	fill(tier, operator, group, keyspace, &[(k.clone(), row("v"))]);
	k
}

fn per_bucket_bytes() -> u64 {
	(BUCKET_OVERHEAD + footprint(&key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), &row("v"))) as u64
}

#[test]
fn only_a_range_that_stays_inside_one_keyspace_is_ever_scoped_to_a_bucket() {
	let single = bucket_scope(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range lives in exactly one bucket");
	assert_eq!(single.bucket.keyspace, Keyspace::ACCUMULATOR);
	assert!(single.whole, "a whole-keyspace range covers its bucket end to end, so a finished scan may fill it");

	let upto = bucket_scope(OP_A, &keyspace_inner_range_upto(GROUP_A, Keyspace::ACCUMULATOR, b"m"))
		.expect("a bounded suffix stays inside the keyspace");
	assert_eq!(upto.bucket.keyspace, Keyspace::ACCUMULATOR);
	assert!(!upto.whole, "a range stopping short of the bucket end must never claim to have filled the bucket");

	let continued = bucket_scope(
		OP_A,
		&EncodedKeyRange::new(
			Bound::Excluded(key(GROUP_A, Keyspace::ACCUMULATOR, b"m")),
			keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR).end,
		),
	)
	.expect("a paged continuation stays inside the keyspace it started in");
	assert!(!continued.whole, "a page that starts past the bucket start never saw the keys before its cursor");

	assert!(
		bucket_scope(OP_A, &group_inner_range(GROUP_A)).is_none(),
		"a whole-group range crosses every keyspace of the group"
	);
	assert!(
		bucket_scope(OP_A, &group_data_inner_range(GROUP_A)).is_none(),
		"the data range crosses every keyspace at or below the data ceiling"
	);
	assert!(
		bucket_scope(OP_A, &group_identity_inner_range(GROUP_A)).is_none(),
		"the identity range crosses every keyspace above the data ceiling"
	);
	assert!(
		bucket_scope(
			OP_A,
			&EncodedKeyRange::new(Bound::Included(EncodedKey::new(Vec::new())), Bound::Unbounded)
		)
		.is_none(),
		"a whole-operator scan reaches the store as an unbounded inner range and can never be one bucket"
	);
	assert!(
		bucket_scope(
			OP_A,
			&EncodedKeyRange::new(
				keyspace_inner_range(GROUP_A, Keyspace::COUNT).start,
				keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR).end,
			)
		)
		.is_none(),
		"a range opening in one keyspace and closing in another must be refused even though both ends parse"
	);
}

#[test]
fn only_a_whole_range_may_begin_a_fill() {
	let tier = roomy();

	assert!(
		tier.begin_fill(OP_A, &keyspace_inner_range_upto(GROUP_A, Keyspace::ACCUMULATOR, b"m")).is_none(),
		"a scan that stopped short of the bucket end must not be allowed to claim the bucket"
	);
	assert!(
		tier.begin_fill(
			OP_A,
			&EncodedKeyRange::new(
				Bound::Excluded(key(GROUP_A, Keyspace::ACCUMULATOR, b"m")),
				keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR).end,
			)
		)
		.is_none(),
		"a continuation page never saw the keys before its cursor, so it cannot claim the bucket"
	);
	assert!(
		tier.begin_fill(OP_A, &group_inner_range(GROUP_A)).is_none(),
		"a range spanning keyspaces has no single bucket to claim"
	);
	assert!(
		tier.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR)).is_some(),
		"the control: a whole-keyspace scan must still be admitted, or the tier is an off switch"
	);
}

#[test]
fn a_resident_bucket_answers_a_range_and_an_absent_one_falls_through() {
	let tier = roomy();
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);

	assert!(tier.range(OP_A, &range, 64).is_none(), "nothing is resident yet, so the read must fall through");
	assert_eq!(tier.metrics().misses, 1);

	let a = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let b = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(a.clone(), row("v1")), (b.clone(), row("v2"))]);

	let served = tier.range(OP_A, &range, 64).expect("a resident bucket must answer its own range");
	assert_eq!(bodies(&served), ["v1", "v2"], "the bucket must serve every row it was filled with, in key order");
	assert_eq!(served[0].0, a);
	assert_eq!(served[1].0, b);
	assert_eq!(tier.metrics().hits, 1);
	assert_eq!(tier.metrics().misses, 1, "a hit must not also charge a miss");
}

#[test]
fn a_range_serves_only_the_slice_it_was_asked_for() {
	let tier = roomy();
	let keys: Vec<EncodedKey> =
		[b"a", b"b", b"c", b"d"].iter().map(|s| key(GROUP_A, Keyspace::ACCUMULATOR, *s)).collect();
	let page: Vec<(EncodedKey, EncodedPodRow)> =
		keys.iter().enumerate().map(|(index, k)| (k.clone(), row(&format!("v{index}")))).collect();
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &page);

	let limited = tier
		.range(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR), 2)
		.expect("the resident bucket must answer");
	assert_eq!(
		bodies(&limited),
		["v0", "v1"],
		"the limit must truncate from the start of the range, not be ignored"
	);

	let sub = tier
		.range(
			OP_A,
			&EncodedKeyRange::new(Bound::Included(keys[1].clone()), Bound::Excluded(keys[3].clone())),
			64,
		)
		.expect("a sub-range of a resident bucket must be answerable from it");
	assert_eq!(bodies(&sub), ["v1", "v2"], "an excluded end must stay excluded and an included start included");

	let empty = tier
		.range(
			OP_A,
			&EncodedKeyRange::new(Bound::Excluded(keys[1].clone()), Bound::Excluded(keys[1].clone())),
			64,
		)
		.expect("a degenerate range over a resident bucket is still answered by it");
	assert!(empty.is_empty(), "a range that excludes both ends of one key selects nothing");
}

#[test]
fn a_lookup_of_a_key_the_resident_bucket_holds_serves_the_row() {
	let tier = roomy();
	let k = one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);

	assert_eq!(tier.lookup(OP_A, &k), Some(Some(row("v"))), "the bucket must hand back the row it holds");
	assert_eq!(tier.metrics().point_hits, 1);
	assert_eq!(tier.metrics().point_misses, 0);
}

#[test]
fn a_lookup_of_a_key_the_resident_bucket_does_not_hold_is_a_definitive_absence() {
	let tier = roomy();
	let held = one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	let absent = key(GROUP_A, Keyspace::ACCUMULATOR, b"zzz");
	assert_ne!(held, absent);

	assert_eq!(
		tier.lookup(OP_A, &absent),
		Some(None),
		"a whole bucket that does not hold the key proves the key does not exist, and reporting a \
		 fall-through instead sends every point read of an absent key to the store forever"
	);
	assert_eq!(tier.metrics().point_hits, 1, "a definitive absence is answered work, not a fall-through");
	assert_eq!(tier.metrics().point_misses, 0);

	let other_bucket = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	assert_eq!(
		tier.lookup(OP_A, &other_bucket),
		None,
		"the answer is only definitive inside the bucket that is resident, never across bucket boundaries"
	);
	assert_eq!(tier.metrics().point_misses, 1);
}

#[test]
fn a_lookup_with_no_resident_bucket_falls_through_and_charges_a_point_miss() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert_eq!(tier.lookup(OP_A, &k), None, "an empty tier must never answer absent for a key the store may hold");
	assert_eq!(tier.metrics().point_misses, 1);
	assert_eq!(tier.metrics().point_hits, 0);
	assert_eq!(tier.metrics().misses, 0, "a point read must not be charged to the range counters");

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	assert!(tier.lookup(OP_A, &k).is_some(), "the control: the same key answers once its bucket is resident");
	assert_eq!(tier.metrics().point_misses, 1, "the fill must not retroactively change the earlier miss");
}

#[test]
fn an_overwrite_never_creates_a_bucket() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("v"));

	assert_eq!(tier.buckets(), 0, "a write against no resident bucket must leave the tier empty");
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a write that cached nothing must be charged nothing");
	assert_eq!(tier.lookup(OP_A, &k), None, "and the key must stay unknown rather than become a false claim");

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[]);
	tier.overwrite(OP_A, k.clone(), row("v"));
	assert_eq!(
		tier.lookup(OP_A, &k),
		Some(Some(row("v"))),
		"the control: the same write must land once a scan made the bucket resident"
	);
}

#[test]
fn invalidating_a_held_key_drops_the_bucket_and_an_unheld_key_keeps_it() {
	let tier = roomy();
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let held = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let unheld = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(held.clone(), row("v"))]);

	tier.invalidate(OP_A, &unheld);
	assert_eq!(
		tier.buckets(),
		1,
		"erasing a key the bucket never held cannot make it short of anything, and dropping the claim here \
		 grinds it away faster than a scan can rebuild it"
	);

	tier.overwrite(OP_A, unheld.clone(), row("w"));
	let served = tier.range(OP_A, &range, 64).expect("the kept claim must still answer the range");
	assert_eq!(
		bodies(&served),
		["v", "w"],
		"a claim kept across a write must answer for the row the flush made durable, or it serves a short \
		 answer that reads as a correct one"
	);

	tier.invalidate(OP_A, &held);
	assert_eq!(
		tier.buckets(),
		0,
		"a write over a key the bucket holds must take the whole claim with it, or the bucket answers \
		 absent for a row the store still has"
	);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "dropping the bucket must release every byte it charged");
	assert!(tier.range(OP_A, &range, 64).is_none(), "and the next range must fall through to the store");
}

#[test]
fn an_invalidate_dirties_the_fill_it_races_so_the_stale_page_cannot_publish() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let bucket =
		tier.begin_fill(OP_A, &range).expect("the fill must start before the write for this to prove anything");
	assert!(tier.extend_fill(bucket, &[(k.clone(), row("stale"))]));

	tier.invalidate(OP_A, &k);

	assert!(!tier.finish_fill(bucket), "a dirtied fill must report that it discarded rather than publish");
	assert_eq!(tier.buckets(), 0, "the discarded page must leave no bucket behind");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
	assert_eq!(tier.metrics().fills_dirty_aborted, 1);
	assert_eq!(tier.metrics().fills, 0);
}

#[test]
fn an_overwrite_dirties_the_fill_it_races_and_extend_notices_it() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let bucket = tier.begin_fill(OP_A, &range).expect("a whole-keyspace range must be fillable");

	tier.overwrite(OP_A, k.clone(), row("flushed"));

	assert!(!tier.extend_fill(bucket, &[(k.clone(), row("pre-flush"))]), "a dirtied fill must refuse more pages");
	assert_eq!(tier.metrics().fills_dirty_aborted, 1);
	assert!(!tier.finish_fill(bucket), "the abandoned fill must not publish through the back of the handshake");
	assert_eq!(tier.buckets(), 0);
}

#[test]
fn a_fill_dirtied_by_an_operator_drop_is_discarded() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, &[(k.clone(), row("stale"))]));

	tier.invalidate_operator(OP_B);
	tier.invalidate_operator(OP_A);

	assert!(!tier.finish_fill(bucket), "a fill whose operator was dropped must discard");
	assert_eq!(tier.buckets(), 0);
	assert_eq!(tier.lookup(OP_A, &k), None);
}

#[test]
fn invalidate_operator_drops_only_its_own_buckets() {
	let tier = roomy();
	let dropped = one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	let spared = one_row_bucket(&tier, OP_B, GROUP_A, Keyspace::ACCUMULATOR);
	assert_eq!(tier.buckets(), 2, "the two operators must own separate buckets, or nothing is under test");

	tier.invalidate_operator(OP_A);

	assert_eq!(tier.lookup(OP_A, &dropped), None, "the dropped operator must keep nothing");
	assert_eq!(tier.lookup(OP_B, &spared), Some(Some(row("v"))), "another operator's bucket must survive intact");
	assert_eq!(tier.buckets(), 1);
	assert_eq!(
		tier.resident_bytes().as_bytes(),
		per_bucket_bytes(),
		"the drop must release exactly the dropped bucket's bytes, not zero and not both buckets"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn clearing_the_tier_discards_every_fill_in_flight() {
	let tier = roomy();
	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, &[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("stale"))]));

	tier.clear();

	assert!(!tier.finish_fill(bucket), "a fill whose marker the clear removed must discard");
	assert_eq!(tier.buckets(), 0);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
}

#[test]
fn a_second_fill_of_one_bucket_is_declined_while_one_is_in_flight() {
	let tier = roomy();
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);

	let first = tier.begin_fill(OP_A, &range).expect("a first fill of an idle bucket must be admitted");
	assert!(tier.begin_fill(OP_A, &range).is_none(), "a second fill of the same bucket must be refused");

	tier.abort_fill(first);
	assert!(tier.begin_fill(OP_A, &range).is_some(), "aborting must free the bucket for the next scan");
}

#[test]
fn an_aborted_fill_leaves_nothing_behind() {
	let tier = roomy();
	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, &[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("v"))]));

	tier.abort_fill(bucket);

	assert_eq!(tier.buckets(), 0);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "an aborted page must never have been charged");
	assert!(!tier.finish_fill(bucket), "finishing an aborted fill must not publish it");
	assert_eq!(tier.metrics().fills, 0);
}

#[test]
fn a_finished_fill_replaces_the_bucket_it_installs_over() {
	let tier = roomy();
	let gone = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let kept = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(gone.clone(), row("v1")), (kept.clone(), row("v2"))]);
	let two_rows = tier.resident_bytes().as_bytes();

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(kept.clone(), row("v2"))]);

	assert_eq!(tier.buckets(), 1, "the rescan must replace the bucket, not add a second one");
	assert_eq!(tier.entries(), 1);
	assert_eq!(tier.lookup(OP_A, &gone), Some(None), "a row the rescan did not see must not survive it");
	assert_eq!(tier.lookup(OP_A, &kept), Some(Some(row("v2"))));
	assert!(tier.resident_bytes().as_bytes() < two_rows, "the replaced copy's bytes must be released");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_page_bigger_than_the_whole_budget_is_declined_while_it_pages() {
	let tier = tier(per_bucket_bytes() * 4);
	let resident = one_row_bucket(&tier, OP_B, GROUP_B, Keyspace::ACCUMULATOR);
	let before = tier.resident_bytes();

	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	let page: Vec<(EncodedKey, EncodedPodRow)> = (0..64u8)
		.map(|index| (key(GROUP_A, Keyspace::ACCUMULATOR, &[index]), row("a fairly long row body")))
		.collect();

	assert!(!tier.extend_fill(bucket, &page), "a page past the shard limit must be refused as it is staged");

	assert_eq!(tier.metrics().fills_declined, 1);
	assert_eq!(tier.metrics().evictions, 0, "a fill that cannot be kept must not evict a bucket that can");
	assert_eq!(tier.buckets(), 1);
	assert_eq!(tier.resident_bytes(), before, "the refused page must not be charged a single byte");
	assert_eq!(tier.lookup(OP_B, &resident), Some(Some(row("v"))), "the resident bucket must be untouched");
	assert!(!tier.finish_fill(bucket), "the abandoned fill must not publish afterwards");
}

#[test]
fn a_fill_that_does_not_fit_beside_the_resident_buckets_is_declined_and_evicts_nothing() {
	let per_bucket = per_bucket_bytes();
	let tier = tier(per_bucket * 2);
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	fill(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket * 2, "the fixture must fill the budget exactly");

	let third = GroupId(12);
	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(third, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, &[(key(third, Keyspace::ACCUMULATOR, b"a"), row("v"))]));

	assert!(!tier.finish_fill(bucket), "a fill that does not fit beside the residents must be declined");

	assert_eq!(tier.metrics().fills_declined, 1);
	assert_eq!(tier.metrics().evictions, 0, "a declined fill must evict nothing");
	assert_eq!(tier.buckets(), 2);
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket * 2, "the declined fill must not be charged");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn growing_past_the_budget_evicts_whole_buckets_and_releases_their_bytes() {
	let per_bucket = per_bucket_bytes();
	let tier = tier(per_bucket * 2);
	let old = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let grown = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(old.clone(), row("v"))]);
	fill(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(grown.clone(), row("v"))]);
	assert_eq!(tier.buckets(), 2, "two buckets fit exactly, so nothing may be evicted yet");
	assert_eq!(tier.metrics().evictions, 0);

	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_eq!(tier.metrics().evictions, 1, "the growth must push exactly one victim out, not the whole shard");
	assert_eq!(tier.buckets(), 1);
	assert!(
		tier.resident_bytes().as_bytes() <= per_bucket * 2,
		"eviction must bring used bytes back under the limit, or the budget stops bounding anything"
	);
	assert_eq!(
		tier.resident_bytes(),
		tier.tallied_bytes(),
		"the budget must equal the sum of the surviving buckets, or eviction released the wrong amount"
	);
	assert_eq!(tier.lookup(OP_A, &old), None, "the least recently touched bucket must be the victim");
	assert!(
		tier.lookup(OP_A, &grown).is_some(),
		"the bucket that triggered eviction must not evict itself while older buckets remain"
	);
}

fn three_bucket_tier() -> (OperatorRangeTier, EncodedKey, EncodedKey, EncodedKey) {
	let tier = tier(per_bucket_bytes() * 3);
	let touched = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let idle = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	let grown = key(GroupId(12), Keyspace::ACCUMULATOR, b"a");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(touched.clone(), row("v"))]);
	fill(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(idle.clone(), row("v"))]);
	fill(&tier, OP_A, GroupId(12), Keyspace::ACCUMULATOR, &[(grown.clone(), row("v"))]);
	(tier, touched, idle, grown)
}

fn assert_idle_bucket_was_the_victim(tier: &OperatorRangeTier, touched: &EncodedKey, idle: &EncodedKey) {
	assert_eq!(tier.metrics().evictions, 1, "the growth must evict exactly one bucket, or the victim is ambiguous");
	assert_eq!(tier.lookup(OP_A, idle), None, "the bucket nothing read since it was installed must be the victim");
	assert!(
		tier.lookup(OP_A, touched).is_some(),
		"a read must refresh the bucket it answered from; leaving the tick stale makes the hottest bucket \
		 the next victim and inverts the LRU into an anti-LRU"
	);
}

#[test]
fn a_range_hit_refreshes_the_bucket_against_eviction() {
	let (tier, touched, idle, grown) = three_bucket_tier();

	assert!(tier.range(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR), 64).is_some());
	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_idle_bucket_was_the_victim(&tier, &touched, &idle);
}

#[test]
fn a_lookup_hit_refreshes_the_bucket_against_eviction() {
	let (tier, touched, idle, grown) = three_bucket_tier();

	assert_eq!(tier.lookup(OP_A, &touched), Some(Some(row("v"))));
	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_idle_bucket_was_the_victim(&tier, &touched, &idle);
}

#[test]
fn charge_and_release_balance_across_the_bucket_lifecycle() {
	let per_bucket = per_bucket_bytes();
	let tier = tier(per_bucket * 2);
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let balanced = |stage: &str| {
		assert_eq!(
			tier.resident_bytes(),
			tier.tallied_bytes(),
			"after {stage} the budget and the per-bucket tally must agree; a drift either way is a leak \
			 that silently shrinks or unbounds the cache"
		);
	};

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket);
	balanced("install");

	tier.overwrite(OP_A, k.clone(), row("a much longer row body"));
	assert!(tier.resident_bytes().as_bytes() > per_bucket, "a larger row must be charged the difference");
	balanced("overwrite with a larger row");

	tier.overwrite(OP_A, k.clone(), row("v"));
	assert_eq!(tier.resident_bytes().as_bytes(), per_bucket, "shrinking an entry must release the difference");
	balanced("overwrite with a smaller row");

	tier.invalidate(OP_A, &k);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "dropping the claim must release the whole bucket");
	balanced("invalidate");

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	balanced("re-install");
	tier.invalidate_operator(OP_A);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "an operator drop must release every byte it removed");
	balanced("operator drop");

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	fill(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	tier.overwrite(OP_A, key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("a very much longer row body indeed"));
	assert!(tier.metrics().evictions > 0, "the fixture must actually evict, or this stage proves nothing");
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

	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(short.clone(), row("v"))]);
	let after_short = tier.resident_bytes().as_bytes();
	tier.overwrite(OP_A, long.clone(), row("v"));

	assert_eq!(
		tier.resident_bytes().as_bytes() - after_short,
		(ENTRY_OVERHEAD + long.heap_bytes() + 1) as u64,
		"a heap allocated key must be charged for its allocation, or long keys are cached for free"
	);
	tier.clear();
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
}

#[test]
fn a_key_too_short_to_carry_a_keyspace_is_declined_not_cached() {
	let tier = roomy();

	for bytes in [vec![], vec![0u8], vec![0u8; 8]] {
		let short = EncodedKey::new(&bytes);
		assert_eq!(BucketId::of(OP_A, &short), None, "a {} byte key cannot carry a bucket", bytes.len());

		tier.overwrite(OP_A, short.clone(), row("v"));
		assert_eq!(tier.lookup(OP_A, &short), None, "a declined key must never be served back");
		tier.invalidate(OP_A, &short);
	}

	assert_eq!(tier.buckets(), 0, "declining must mean not caching, not caching under a wrong bucket");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a declined key must not be charged");
	assert_eq!(
		tier.metrics().point_misses,
		0,
		"an undecodable key is not attributable to a shard, so it counts as neither a hit nor a miss"
	);
	assert_eq!(tier.metrics().point_hits, 0);

	let shortest_valid = key(GROUP_A, Keyspace::ACCUMULATOR, b"");
	assert_eq!(shortest_valid.len(), 9, "group plus keyspace with an empty suffix is the shortest valid key");
	assert!(BucketId::of(OP_A, &shortest_valid).is_some(), "the shortest valid key must not be declined");
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
		let range = keyspace_inner_range(GROUP_A, keyspace);
		let k = key(GROUP_A, keyspace, b"a");

		assert!(
			tier.begin_fill(OP_A, &range).is_none(),
			"{} must be refused before the scan starts; a fill that can only be thrown away still takes \
			 the shard lock and pays for the scan",
			keyspace.name()
		);
		assert!(
			!tier.finish_fill(BucketId::of(OP_A, &k).expect("the fixture key carries a bucket")),
			"a fill that was never admitted must not publish {} through the back of the handshake",
			keyspace.name()
		);
		tier.overwrite(OP_A, k.clone(), row("v"));

		assert!(
			tier.range(OP_A, &range, 64).is_none(),
			"{} must stay unknown so the read falls through to the store",
			keyspace.name()
		);
		assert_eq!(tier.lookup(OP_A, &k), None, "{} must answer no point read either", keyspace.name());
	}

	assert_eq!(tier.buckets(), 0, "an excluded keyspace must not occupy a bucket, not even an empty one");
	assert_eq!(tier.entries(), 0, "an excluded keyspace must leave no entry behind");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);

	assert!(
		tier.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR)).is_some(),
		"the control: a gate that refused every keyspace would pass the assertions above while turning the \
		 whole tier into an off switch, and that only shows up as a throughput loss in a replay"
	);
}

#[test]
fn a_tier_without_a_byte_budget_is_not_constructed() {
	assert!(OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: None,
		shards: 16,
	})
	.is_none());
	assert!(OperatorRangeTier::new(OperatorRangeConfig::default()).is_some());
	assert_eq!(OperatorRangeConfig::default().shards, 16);
	assert_eq!(OperatorRangeConfig::default().resident_bytes, Some(ByteSize::from_mib(64)));
}

#[test]
fn every_shard_is_reachable_and_reports_its_own_slice_of_the_budget() {
	let tier = OperatorRangeTier::new(OperatorRangeConfig {
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
	assert_eq!(tier.shard_limit_bytes(), ByteSize::from_mib(16));

	for group in 0..64u64 {
		one_row_bucket(&tier, OP_A, GroupId(group), Keyspace::ACCUMULATOR);
	}
	assert_eq!(tier.buckets(), 64);
	assert!(
		tier.shard_metrics().iter().all(|shard| shard.buckets > 0),
		"64 buckets must reach all 4 shards, or the shard hash ignores part of the bucket id"
	);
	assert_eq!(tier.shard_metrics().iter().map(|shard| shard.entries).sum::<usize>(), tier.entries());
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

fn keyspace_row(tier: &OperatorRangeTier, keyspace: Keyspace) -> OperatorRangeKeyspaceMetrics {
	tier.keyspace_metrics()
		.into_iter()
		.find(|row| row.keyspace == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
}

#[test]
fn keyspace_counters_are_charged_to_the_keyspace_that_was_read() {
	let tier = roomy();
	let accumulator_range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let buffer_range = keyspace_inner_range(GROUP_A, Keyspace::BUFFER);

	assert!(tier.range(OP_A, &accumulator_range, 64).is_none());
	one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	assert!(tier.range(OP_A, &accumulator_range, 64).is_some());
	assert!(tier.range(OP_A, &buffer_range, 64).is_none());

	assert_eq!(
		tier.keyspace_metrics().len(),
		2,
		"only the two keyspaces that were touched may be reported; a fixed 256 slot table must never \
		 surface as 256 rows of zeros"
	);

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.hits, 1);
	assert_eq!(accumulator.counters.misses, 1);
	assert_eq!(accumulator.counters.fills, 1);
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
fn point_counters_are_charged_to_the_keyspace_that_was_looked_up() {
	let tier = roomy();
	let known = one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::EMIT);
	let unknown = key(GROUP_A, Keyspace::JOIN_LEFT, b"a");

	assert_eq!(tier.lookup(OP_A, &known), Some(Some(row("v"))));
	assert_eq!(tier.lookup(OP_A, &key(GROUP_A, Keyspace::EMIT, b"zzz")), Some(None));
	assert_eq!(tier.lookup(OP_A, &unknown), None);

	assert_eq!(keyspace_row(&tier, Keyspace::EMIT).counters.point_hits, 2);
	assert_eq!(keyspace_row(&tier, Keyspace::EMIT).counters.point_misses, 0);
	assert_eq!(keyspace_row(&tier, Keyspace::JOIN_LEFT).counters.point_misses, 1);
	assert_eq!(keyspace_row(&tier, Keyspace::JOIN_LEFT).counters.point_hits, 0);
	assert_eq!(keyspace_row(&tier, Keyspace::EMIT).counters.hits, 0, "a point read is not a range hit");
}

#[test]
fn resident_state_is_grouped_by_keyspace_and_sums_to_the_tier_total() {
	let tier = roomy();
	let per_bucket = per_bucket_bytes();

	one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	one_row_bucket(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR);
	one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::BUFFER);

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.buckets, 2);
	assert_eq!(accumulator.entries, 2);
	assert_eq!(accumulator.used, ByteSize::from_bytes(per_bucket * 2));

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.buckets, 1);
	assert_eq!(buffer.entries, 1);
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
	let tier = OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_mib(64)),
		shards: 4,
	})
	.expect("a sharded tier must be constructed");

	for group in 0..64u64 {
		one_row_bucket(&tier, OP_A, GroupId(group), Keyspace::SOURCE_WATERMARK);
	}
	for group in 0..64u64 {
		assert!(tier
			.range(OP_A, &keyspace_inner_range(GroupId(group), Keyspace::SOURCE_WATERMARK), 64)
			.is_some());
	}

	assert!(
		tier.shard_metrics().iter().filter(|shard| shard.counters.hits > 0).count() > 1,
		"the fixture must spread hits over more than one shard, or summation is not under test"
	);

	let reported = tier.keyspace_metrics();
	assert_eq!(reported.len(), 1, "one keyspace spread over four shards must collapse to a single row");
	assert_eq!(reported[0].keyspace, Keyspace::SOURCE_WATERMARK);
	assert_eq!(reported[0].counters.hits, 64);
	assert_eq!(reported[0].counters.fills, 64);
	assert_eq!(reported[0].buckets, 64);
	assert_eq!(reported[0].entries, 64);
}

#[test]
fn an_eviction_is_charged_to_the_evicted_bucket_keyspace() {
	let tier = tier(per_bucket_bytes() * 2);
	let accumulator = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let buffer = key(GROUP_A, Keyspace::BUFFER, b"a");
	fill(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(accumulator.clone(), row("v"))]);
	fill(&tier, OP_A, GROUP_A, Keyspace::BUFFER, &[(buffer.clone(), row("v"))]);

	tier.overwrite(OP_A, buffer.clone(), row("a very much longer row body than the one it replaces"));

	assert_eq!(tier.metrics().evictions, 1, "the fixture must evict, or the attribution below proves nothing");
	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.evictions, 1);
	assert_eq!(accumulator.buckets, 0, "the evicted bucket must be gone from its keyspace's resident state");

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.evictions, 0, "the survivor must not be charged for the victim's eviction");
	assert_eq!(buffer.buckets, 1);
}

#[test]
fn a_tier_that_was_never_read_reports_no_keyspace_rows() {
	let tier = roomy();
	assert!(tier.keyspace_metrics().is_empty());

	let resident = one_row_bucket(&tier, OP_A, GROUP_A, Keyspace::JOIN_LEFT);
	assert_eq!(tier.keyspace_metrics().len(), 1, "resident state alone must be enough to report a keyspace");
	assert_eq!(tier.keyspace_metrics()[0].keyspace, Keyspace::JOIN_LEFT);

	assert!(tier.lookup(OP_A, &resident).is_some());
	tier.clear();
	assert_eq!(
		tier.keyspace_metrics().len(),
		1,
		"clearing drops resident state but not the counters, so a keyspace with history stays reported"
	);
	assert_eq!(tier.keyspace_metrics()[0].buckets, 0);
	assert_ne!(tier.keyspace_metrics()[0].counters, OperatorRangeMetrics::default());
}

#[test]
fn finish_fill_publishes_under_the_lock_that_cleared_the_marker() {
	let acquired = Arc::new(AtomicBool::new(false));
	let probed = Arc::new(AtomicBool::new(false));
	let flag = acquired.clone();
	let seen = probed.clone();
	let hook: FillInterlock = Box::new(move |tier: &OperatorRangeTier, id: BucketId| {
		seen.store(true, Ordering::Relaxed);
		flag.store(tier.shard_for(&id).try_lock().is_some(), Ordering::Relaxed);
	});

	let tier = OperatorRangeTier::with_interlock(
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");

	let bucket = tier
		.begin_fill(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR))
		.expect("a whole-keyspace range must be fillable");
	assert!(tier.extend_fill(bucket, &[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("v"))]));
	assert!(tier.finish_fill(bucket), "an undirtied fill must publish");

	assert!(probed.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert!(
		!acquired.load(Ordering::Relaxed),
		"the shard lock was acquirable between clearing the fill marker and publishing the bucket; a writer \
		 invalidating in that window is silently dropped and the stale bucket is published over it"
	);
}
