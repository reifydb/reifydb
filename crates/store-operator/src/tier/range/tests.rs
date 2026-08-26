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
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, group_inner_range, keyspace_inner_range},
};
use reifydb_store::coverage::{
	cursor::{RangeCursor, ServedChunk},
	interval::Interval,
	plan::{DEFAULT_GAP_GUARD, Segment},
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::{
	ENTRY_OVERHEAD, Materialize, MaterializeInterlock, OperatorRangeConfig, OperatorRangeKeyspaceMetrics,
	OperatorRangeMetrics, OperatorRangeTier, PARTITION_OVERHEAD, PartitionId, RangeScan,
};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tier(limit: u64) -> OperatorRangeTier {
	OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_bytes(limit)),
		shards: 1,
		gap_guard: DEFAULT_GAP_GUARD,
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

fn per_partition_bytes() -> u64 {
	(PARTITION_OVERHEAD + footprint(&key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), &row("v"))) as u64
}

fn materialize(
	tier: &OperatorRangeTier,
	operator: OperatorId,
	group: GroupId,
	keyspace: Keyspace,
	page: &[(EncodedKey, EncodedPodRow)],
) -> PartitionId {
	let range = keyspace_inner_range(group, keyspace);
	let scan = tier.plan_scan(operator, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap the fixture can materialize over");
	assert!(
		tier.materialize(&scan, &gap, page) == Materialize::Materialized,
		"the fixture page must fit the materialize it is staging"
	);
	PartitionId {
		operator,
		group,
		keyspace,
	}
}

fn first_gap(scan: &RangeScan) -> Option<Interval> {
	scan.segments().iter().find_map(|segment| match segment {
		Segment::Gap {
			interval,
			..
		} => Some(interval.clone()),
		Segment::Resident(_) => None,
	})
}

fn one_row_partition(tier: &OperatorRangeTier, operator: OperatorId, group: GroupId, keyspace: Keyspace) -> EncodedKey {
	let k = key(group, keyspace, b"a");
	materialize(tier, operator, group, keyspace, &[(k.clone(), row("v"))]);
	k
}

fn serve_ram(
	tier: &OperatorRangeTier,
	operator: OperatorId,
	range: &EncodedKeyRange,
	limit: usize,
) -> Option<Vec<(EncodedKey, EncodedPodRow)>> {
	let scan = tier.plan_scan(operator, range)?;
	let mut out: Vec<(EncodedKey, EncodedPodRow)> = Vec::new();
	let mut resident = false;

	for segment in scan.segments() {
		let Segment::Resident(interval) = segment else {
			continue;
		};
		resident = true;
		let mut cursor = RangeCursor::new();
		while !cursor.is_exhausted() && out.len() < limit {
			let before = out.len();
			match tier.serve(&scan, interval, &mut cursor, limit - out.len()) {
				ServedChunk::Served(rows) => out.extend(rows),
				ServedChunk::Gap => break,
			}
			assert!(
				cursor.is_exhausted() || out.len() > before,
				"a chunk that reports more work must carry a row, or the cursor never advances and the scan \
                 loop spins forever"
			);
		}
		if out.len() >= limit {
			break;
		}
	}

	resident.then_some(out)
}

fn covers(tier: &OperatorRangeTier, operator: OperatorId, range: &EncodedKeyRange) -> bool {
	tier.plan_scan(operator, range)
		.map(|scan| scan.segments().iter().any(|segment| matches!(segment, Segment::Resident(_))))
		.unwrap_or(false)
}

#[test]
fn a_covered_span_answers_a_range_and_an_uncovered_one_falls_through() {
	let tier = roomy();
	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);

	assert!(serve_ram(&tier, OP_A, &range, 64).is_none(), "nothing is covered yet, so the read must fall through");
	assert_eq!(tier.metrics().misses, 1);

	let a = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let b = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(a.clone(), row("v1")), (b.clone(), row("v2"))]);

	let served = serve_ram(&tier, OP_A, &range, 64).expect("a covered span must answer its own range");
	assert_eq!(bodies(&served), ["v1", "v2"], "the claim must serve every row it was materialized with, in key order");
	assert_eq!(served[0].0, a);
	assert_eq!(served[1].0, b);
	assert_eq!(tier.metrics().hits, 1);
	assert_eq!(
		tier.metrics().misses,
		2,
		"the fixture plans twice, so both gaps are charged; a hit must add a hit and rescind nothing"
	);
}

#[test]
fn a_range_serves_only_the_slice_it_was_asked_for() {
	let tier = roomy();
	let keys: Vec<EncodedKey> =
		[b"a", b"b", b"c", b"d"].iter().map(|s| key(GROUP_A, Keyspace::ACCUMULATOR, *s)).collect();
	let page: Vec<(EncodedKey, EncodedPodRow)> =
		keys.iter().enumerate().map(|(index, k)| (k.clone(), row(&format!("v{index}")))).collect();
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &page);

	let limited = serve_ram(&tier, OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR), 2)
		.expect("the covered span must answer");
	assert_eq!(
		bodies(&limited),
		["v0", "v1"],
		"the limit must truncate from the start of the range, not be ignored"
	);

	let sub = serve_ram(
		&tier,
		OP_A,
		&EncodedKeyRange::new(Bound::Included(keys[1].clone()), Bound::Excluded(keys[3].clone())),
		64,
	)
	.expect("a sub-range of a covered span must be answerable from it");
	assert_eq!(bodies(&sub), ["v1", "v2"], "an excluded end must stay excluded and an included start included");

	let empty = serve_ram(
		&tier,
		OP_A,
		&EncodedKeyRange::new(Bound::Excluded(keys[1].clone()), Bound::Excluded(keys[1].clone())),
		64,
	)
	.expect("a degenerate range over a covered span is still answered by it");
	assert!(empty.is_empty(), "a range that excludes both ends of one key selects nothing");
}

#[test]
fn a_lookup_of_a_key_the_claim_holds_serves_the_row() {
	let tier = roomy();
	let k = one_row_partition(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);

	assert_eq!(tier.lookup(OP_A, &k), Some(Some(row("v"))), "the claim must hand back the row it holds");
	assert_eq!(tier.metrics().point_hits, 1);
	assert_eq!(tier.metrics().point_misses, 0);
}

#[test]
fn a_lookup_of_a_key_inside_a_claim_that_holds_no_row_is_a_definitive_absence() {
	let tier = roomy();
	let held = one_row_partition(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	let absent = key(GROUP_A, Keyspace::ACCUMULATOR, b"zzz");
	assert_ne!(held, absent);

	assert_eq!(
		tier.lookup(OP_A, &absent),
		Some(None),
		"a claim that covers the key and holds no row proves the key does not exist, and reporting a \
         fall-through instead sends every point read of an absent key to the store forever"
	);
	assert_eq!(tier.metrics().point_hits, 1, "a definitive absence is answered work, not a fall-through");
	assert_eq!(tier.metrics().point_misses, 0);

	let uncovered = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	assert_eq!(
		tier.lookup(OP_A, &uncovered),
		None,
		"the answer is only definitive inside a span some scan actually proved, never outside it"
	);
	assert_eq!(tier.metrics().point_misses, 1);
}

#[test]
fn a_lookup_with_nothing_covered_falls_through_and_charges_a_point_miss() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert_eq!(tier.lookup(OP_A, &k), None, "an empty tier must never answer absent for a key the store may hold");
	assert_eq!(tier.metrics().point_misses, 1);
	assert_eq!(tier.metrics().point_hits, 0);
	assert_eq!(tier.metrics().misses, 0, "a point read must not be charged to the range counters");

	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	assert!(tier.lookup(OP_A, &k).is_some(), "the control: the same key answers once a scan covered it");
	assert_eq!(tier.metrics().point_misses, 1, "the materialize must not retroactively change the earlier miss");
}

#[test]
fn an_overwrite_never_creates_a_claim() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("v"));

	assert_eq!(tier.partitions(), 0, "a write against no claim must leave the tier empty");
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.intervals(), 0, "an overwrite must never widen coverage to keys no scan observed");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a write that cached nothing must be charged nothing");
	assert_eq!(tier.lookup(OP_A, &k), None, "and the key must stay unknown rather than become a false claim");

	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[]);
	tier.overwrite(OP_A, k.clone(), row("v"));
	assert_eq!(
		tier.lookup(OP_A, &k),
		Some(Some(row("v"))),
		"the control: the same write must land once a scan claimed the span it falls in"
	);
}

#[test]
fn invalidate_operator_drops_only_its_own_claims() {
	let tier = roomy();
	let dropped = one_row_partition(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	let spared = one_row_partition(&tier, OP_B, GROUP_A, Keyspace::ACCUMULATOR);
	assert_eq!(tier.partitions(), 2, "the two operators must own separate partitions, or nothing is under test");

	tier.invalidate_operator(OP_A);

	assert_eq!(tier.lookup(OP_A, &dropped), None, "the dropped operator must keep nothing");
	assert_eq!(tier.lookup(OP_B, &spared), Some(Some(row("v"))), "another operator's claim must survive intact");
	assert_eq!(tier.partitions(), 1);
	assert_eq!(
		tier.resident_bytes().as_bytes(),
		per_partition_bytes(),
		"the drop must release exactly the dropped partition's bytes, not zero and not both"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_materialize_keeps_a_row_already_resident_rather_than_replacing_it() {
	let tier = roomy();
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v1"))]);

	let range = keyspace_inner_range(GROUP_B, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("an uncovered keyspace must be plannable");
	tier.overwrite(OP_A, k.clone(), row("v2"));
	let gap = first_gap(&scan).expect("the uncovered keyspace must plan as a gap");
	tier.materialize(&scan, &gap, &[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("other"))]);

	assert_eq!(
		tier.lookup(OP_A, &k),
		Some(Some(row("v2"))),
		"a resident row is at least as new as any persistent read, so a materialize must never undo a write \
         that landed while that read was in flight"
	);
}

#[test]
fn a_materialize_that_does_not_fit_the_budget_is_refused_whole_and_evicts_nothing() {
	let per_partition = per_partition_bytes();
	let tier = tier(per_partition * 2);
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	materialize(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	assert_eq!(tier.resident_bytes().as_bytes(), per_partition * 2, "the fixture must fill the budget exactly");
	let before = tier.resident_bytes();

	let third = GroupId(12);
	let range = keyspace_inner_range(third, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("the uncovered keyspace must plan as a gap");
	let page: Vec<(EncodedKey, EncodedPodRow)> = (0..64u8)
		.map(|index| (key(third, Keyspace::ACCUMULATOR, &[index]), row("a fairly long row body")))
		.collect();

	assert!(tier.materialize(&scan, &gap, &page) == Materialize::Refused, "a span past the shard limit must be refused");

	assert_eq!(tier.metrics().materializes_refused, 1);
	assert_eq!(tier.metrics().evictions, 0, "a new claim must never evict a proven resident to make room");
	assert_eq!(tier.partitions(), 2);
	assert_eq!(tier.resident_bytes(), before, "a refused materialize must not be charged a single byte");
	assert_eq!(
		tier.lookup(OP_A, &key(third, Keyspace::ACCUMULATOR, &[0u8])),
		None,
		"a refused materialize must roll its rows back, or a later read answers from a row no claim ever proved"
	);
	assert!(!covers(&tier, OP_A, &range), "and it must leave no claim over the span it failed to materialize");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn growing_past_the_budget_evicts_a_whole_partition_and_releases_its_bytes() {
	let per_partition = per_partition_bytes();
	let tier = tier(per_partition * 2);
	let old = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let grown = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(old.clone(), row("v"))]);
	materialize(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(grown.clone(), row("v"))]);
	assert_eq!(tier.partitions(), 2, "two partitions fit exactly, so nothing may be evicted yet");
	assert_eq!(tier.metrics().evictions, 0);

	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_eq!(tier.metrics().evictions, 1, "the growth must push exactly one victim out, not the whole shard");
	assert_eq!(tier.partitions(), 1);
	assert!(
		tier.resident_bytes().as_bytes() <= per_partition * 2,
		"eviction must bring used bytes back under the limit, or the budget stops bounding anything"
	);
	assert_eq!(
		tier.resident_bytes(),
		tier.tallied_bytes(),
		"the budget must equal the sum of the survivors, or eviction released the wrong amount"
	);
	assert_eq!(
		tier.lookup(OP_A, &old),
		None,
		"the evicted span must fall through, never answer a proven absence: eviction that drops rows while \
         leaving the claim standing turns every evicted row into silent wrong data"
	);
	assert!(tier.lookup(OP_A, &grown).is_some(), "the partition that triggered eviction must not evict itself");
}

fn three_partition_tier() -> (OperatorRangeTier, EncodedKey, EncodedKey, EncodedKey) {
	let tier = tier(per_partition_bytes() * 3);
	let touched = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let idle = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	let grown = key(GroupId(12), Keyspace::ACCUMULATOR, b"a");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(touched.clone(), row("v"))]);
	materialize(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(idle.clone(), row("v"))]);
	materialize(&tier, OP_A, GroupId(12), Keyspace::ACCUMULATOR, &[(grown.clone(), row("v"))]);
	(tier, touched, idle, grown)
}

fn assert_idle_partition_was_the_victim(tier: &OperatorRangeTier, touched: &EncodedKey, idle: &EncodedKey) {
	assert_eq!(
		tier.metrics().evictions,
		1,
		"the growth must evict exactly one partition, or the victim is ambiguous"
	);
	assert_eq!(
		tier.lookup(OP_A, idle),
		None,
		"the partition nothing read since it was materialized must be the victim"
	);
	assert!(
		tier.lookup(OP_A, touched).is_some(),
		"a read must refresh the partition it answered from; leaving the tick stale makes the hottest partition \
         the next victim and inverts the LRU into an anti-LRU"
	);
}

#[test]
fn a_range_hit_refreshes_the_partition_against_eviction() {
	let (tier, touched, idle, grown) = three_partition_tier();

	assert!(serve_ram(&tier, OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR), 64).is_some());
	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_idle_partition_was_the_victim(&tier, &touched, &idle);
}

#[test]
fn a_lookup_hit_refreshes_the_partition_against_eviction() {
	let (tier, touched, idle, grown) = three_partition_tier();

	assert_eq!(tier.lookup(OP_A, &touched), Some(Some(row("v"))));
	tier.overwrite(OP_A, grown.clone(), row("a very much longer row body than the one it replaces"));

	assert_idle_partition_was_the_victim(&tier, &touched, &idle);
}

#[test]
fn charge_and_release_balance_across_the_partition_lifecycle() {
	let per_partition = per_partition_bytes();
	let tier = tier(per_partition * 2);
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let balanced = |stage: &str| {
		assert_eq!(
			tier.resident_bytes(),
			tier.tallied_bytes(),
			"after {stage} the budget and the per-partition tally must agree; a drift either way is a leak \
             that silently shrinks or unbounds the cache"
		);
	};

	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	assert_eq!(tier.resident_bytes().as_bytes(), per_partition);
	balanced("materialize");

	tier.overwrite(OP_A, k.clone(), row("a much longer row body"));
	assert!(tier.resident_bytes().as_bytes() > per_partition, "a larger row must be charged the difference");
	balanced("overwrite with a larger row");

	tier.overwrite(OP_A, k.clone(), row("v"));
	assert_eq!(tier.resident_bytes().as_bytes(), per_partition, "shrinking an entry must release the difference");
	balanced("overwrite with a smaller row");

	tier.invalidate_operator(OP_A);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "an operator drop must release every byte it removed");
	balanced("operator drop");

	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(k.clone(), row("v"))]);
	materialize(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))]);
	tier.overwrite(OP_A, key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("a very much longer row body indeed"));
	assert!(tier.metrics().evictions > 0, "the fixture must actually evict, or this stage proves nothing");
	balanced("evict");

	tier.invalidate_operator(OP_A);
	assert_eq!(
		tier.resident_bytes(),
		ByteSize::ZERO,
		"a purge must release every byte it charged rather than zero the counter, or a leak stays invisible"
	);
	assert_eq!(tier.partitions(), 0);
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.intervals(), 0, "a purge must take the coverage with it, or a claim outlives its rows");
}

#[test]
fn a_long_key_charges_its_heap_bytes() {
	let tier = roomy();
	let short = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let long = key(GROUP_A, Keyspace::ACCUMULATOR, &[7u8; 64]);
	assert_eq!(short.heap_bytes(), 0, "the short fixture must stay inline, or the comparison below is meaningless");
	assert!(long.heap_bytes() > 0, "the long fixture must spill to the heap, or nothing tests heap accounting");

	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(short.clone(), row("v"))]);
	let after_short = tier.resident_bytes().as_bytes();
	tier.overwrite(OP_A, long.clone(), row("v"));

	assert_eq!(
		tier.resident_bytes().as_bytes() - after_short,
		(ENTRY_OVERHEAD + long.heap_bytes() + 1) as u64,
		"a heap allocated key must be charged for its allocation, or long keys are cached for free"
	);
	tier.invalidate_operator(OP_A);
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
}

#[test]
fn a_key_too_short_to_carry_a_keyspace_is_declined_not_cached() {
	let tier = roomy();

	for bytes in [vec![], vec![0u8], vec![0u8; 8]] {
		let short = EncodedKey::new(&bytes);
		assert_eq!(PartitionId::of(OP_A, &short), None, "a {} byte key cannot carry a partition", bytes.len());

		tier.overwrite(OP_A, short.clone(), row("v"));
		tier.insert(OP_A, short.clone(), row("v"));
		assert_eq!(tier.lookup(OP_A, &short), None, "a declined key must never be served back");
		tier.mark_deleted(OP_A, &short);
	}

	assert_eq!(tier.partitions(), 0, "declining must mean not caching, not caching under a wrong partition");
	assert_eq!(tier.intervals(), 0, "and a key with no partition must never be claimed as an island");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a declined key must not be charged");
	assert_eq!(
		tier.metrics().point_misses,
		0,
		"an undecodable key is not attributable to a shard, so it counts as neither a hit nor a miss"
	);
	assert_eq!(tier.metrics().point_hits, 0);

	let shortest_valid = key(GROUP_A, Keyspace::ACCUMULATOR, b"");
	assert_eq!(shortest_valid.len(), 9, "group plus keyspace with an empty suffix is the shortest valid key");
	assert!(PartitionId::of(OP_A, &shortest_valid).is_some(), "the shortest valid key must not be declined");
}

const EXCLUDED: [Keyspace; 1] = [Keyspace::CUSTOM_NOT_CACHED];

#[test]
fn no_admission_path_lets_an_excluded_keyspace_into_the_tier() {
	let tier = roomy();

	for keyspace in EXCLUDED {
		let range = keyspace_inner_range(GROUP_A, keyspace);
		let k = key(GROUP_A, keyspace, b"a");

		assert!(
			tier.plan_scan(OP_A, &range).is_none(),
			"{} must be refused before the scan starts; a plan that can only be thrown away still takes the \
             shard lock and pays for the scan",
			keyspace.name()
		);
		tier.overwrite(OP_A, k.clone(), row("v"));
		tier.insert(OP_A, k.clone(), row("v"));

		assert!(
			serve_ram(&tier, OP_A, &range, 64).is_none(),
			"{} must stay unknown so the read falls through to the store",
			keyspace.name()
		);
		assert_eq!(tier.lookup(OP_A, &k), None, "{} must answer no point read either", keyspace.name());
	}

	assert_eq!(tier.partitions(), 0, "an excluded keyspace must not occupy a partition, not even an empty one");
	assert_eq!(tier.entries(), 0, "an excluded keyspace must leave no entry behind");
	assert_eq!(tier.intervals(), 0, "and no claim, not even a one-key island from the write path");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO);

	assert!(
		tier.plan_scan(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR)).is_some(),
		"the control: a gate that refused every keyspace would pass the assertions above while turning the \
         whole tier into an off switch, and that only shows up as a throughput loss in a replay"
	);
}

#[test]
fn a_gap_over_an_excluded_keyspace_never_degrades_the_plan() {
	let tier = roomy();
	let range = group_inner_range(GROUP_A);

	let scan = tier.plan_scan(OP_A, &range).expect("a whole-group range must be plannable");

	assert!(
		!scan.degraded(),
		"every excluded keyspace in the group is a gap, and counting those against the guard would degrade \
         every group-wide scan to one full read forever"
	);
	assert!(
		scan.gaps() <= DEFAULT_GAP_GUARD,
		"only the cacheable keyspaces may count toward the guard, or the exemption is not being applied"
	);
}

#[test]
fn a_tier_without_a_byte_budget_is_not_constructed() {
	assert!(OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: None,
		shards: 16,
		gap_guard: DEFAULT_GAP_GUARD,
	})
	.is_none());
	assert!(OperatorRangeTier::new(OperatorRangeConfig::default()).is_some());
	assert_eq!(OperatorRangeConfig::default().shards, 16);
	assert_eq!(OperatorRangeConfig::default().resident_bytes, Some(ByteSize::from_mib(64)));
	assert_eq!(OperatorRangeConfig::default().gap_guard, DEFAULT_GAP_GUARD);
}

#[test]
fn every_shard_is_reachable_and_reports_its_own_slice_of_the_budget() {
	let tier = OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_mib(64)),
		shards: 4,
		gap_guard: DEFAULT_GAP_GUARD,
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
		one_row_partition(&tier, OP_A, GroupId(group), Keyspace::ACCUMULATOR);
	}
	assert_eq!(tier.partitions(), 64);
	assert!(
		tier.shard_metrics().iter().all(|shard| shard.partitions > 0),
		"64 partitions must reach all 4 shards, or the shard hash ignores part of the partition id"
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

	assert!(serve_ram(&tier, OP_A, &accumulator_range, 64).is_none());
	one_row_partition(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	assert!(serve_ram(&tier, OP_A, &accumulator_range, 64).is_some());
	assert!(serve_ram(&tier, OP_A, &buffer_range, 64).is_none());

	assert_eq!(
		tier.keyspace_metrics().len(),
		2,
		"only the two keyspaces that were touched may be reported; a fixed 256 slot table must never \
         surface as 256 rows of zeros"
	);

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.hits, 1);
	assert_eq!(
		accumulator.counters.misses, 2,
		"a gap stays charged once it is handed to the store; a materialize may never take its miss back"
	);
	assert_eq!(accumulator.counters.materializes, 1);
	assert_eq!(accumulator.partitions, 1);
	assert_eq!(accumulator.entries, 1);

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.hits, 0, "a miss in one keyspace must not borrow the other keyspace's hit");
	assert_eq!(buffer.counters.misses, 1);
	assert_eq!(
		buffer.partitions, 0,
		"a keyspace with no resident partition is still reported once a counter moved"
	);

	assert_eq!(tier.metrics().hits, 1, "the per shard aggregate must survive alongside the keyspace table");
	assert_eq!(
		tier.metrics().misses,
		3,
		"the aggregate is the sum of the keyspace rows: two accumulator gaps and one buffer gap"
	);
}

#[test]
fn point_counters_are_charged_to_the_keyspace_that_was_looked_up() {
	let tier = roomy();
	let known = one_row_partition(&tier, OP_A, GROUP_A, Keyspace::EMIT);
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
	let per_partition = per_partition_bytes();

	one_row_partition(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR);
	one_row_partition(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR);
	one_row_partition(&tier, OP_A, GROUP_A, Keyspace::BUFFER);

	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.partitions, 2);
	assert_eq!(accumulator.intervals, 2);
	assert_eq!(accumulator.entries, 2);
	assert_eq!(accumulator.used, ByteSize::from_bytes(per_partition * 2));

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.partitions, 1);
	assert_eq!(buffer.intervals, 1);
	assert_eq!(buffer.entries, 1);
	assert_eq!(buffer.used, ByteSize::from_bytes(per_partition));

	let total: u64 = tier.keyspace_metrics().iter().map(|row| row.used.as_bytes()).sum();
	assert_eq!(
		ByteSize::from_bytes(total),
		tier.tallied_bytes(),
		"every resident byte must be attributed to exactly one keyspace, or the table leaks or double counts"
	);
	assert_eq!(tier.keyspace_metrics().iter().map(|row| row.partitions).sum::<usize>(), tier.partitions());
	assert!(
		tier.keyspace_metrics().iter().map(|row| row.intervals).sum::<usize>() >= tier.intervals(),
		"every claim must be counted in at least one keyspace, or a fragmenting keyspace reports none"
	);
	assert_eq!(tier.keyspace_metrics().iter().map(|row| row.entries).sum::<usize>(), tier.entries());
}

#[test]
fn keyspace_counters_are_summed_across_every_shard() {
	let tier = OperatorRangeTier::new(OperatorRangeConfig {
		resident_bytes: Some(ByteSize::from_mib(64)),
		shards: 4,
		gap_guard: DEFAULT_GAP_GUARD,
	})
	.expect("a sharded tier must be constructed");

	for group in 0..64u64 {
		one_row_partition(&tier, OP_A, GroupId(group), Keyspace::SOURCE_WATERMARK);
	}
	for group in 0..64u64 {
		assert!(serve_ram(&tier, OP_A, &keyspace_inner_range(GroupId(group), Keyspace::SOURCE_WATERMARK), 64)
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
	assert_eq!(reported[0].counters.materializes, 64);
	assert_eq!(reported[0].partitions, 64);
	assert_eq!(reported[0].entries, 64);
}

#[test]
fn an_eviction_is_charged_to_the_evicted_partition_keyspace() {
	let tier = tier(per_partition_bytes() * 2);
	let accumulator = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let buffer = key(GROUP_A, Keyspace::BUFFER, b"a");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(accumulator.clone(), row("v"))]);
	materialize(&tier, OP_A, GROUP_A, Keyspace::BUFFER, &[(buffer.clone(), row("v"))]);

	tier.overwrite(OP_A, buffer.clone(), row("a very much longer row body than the one it replaces"));

	assert_eq!(tier.metrics().evictions, 1, "the fixture must evict, or the attribution below proves nothing");
	let accumulator = keyspace_row(&tier, Keyspace::ACCUMULATOR);
	assert_eq!(accumulator.counters.evictions, 1);
	assert_eq!(accumulator.partitions, 0, "the evicted partition must be gone from its keyspace's resident state");

	let buffer = keyspace_row(&tier, Keyspace::BUFFER);
	assert_eq!(buffer.counters.evictions, 0, "the survivor must not be charged for the victim's eviction");
	assert_eq!(buffer.partitions, 1);
}

#[test]
fn a_tier_that_was_never_read_reports_no_keyspace_rows() {
	let tier = roomy();
	assert!(tier.keyspace_metrics().is_empty());

	let resident = one_row_partition(&tier, OP_A, GROUP_A, Keyspace::JOIN_LEFT);
	assert_eq!(tier.keyspace_metrics().len(), 1, "resident state alone must be enough to report a keyspace");
	assert_eq!(tier.keyspace_metrics()[0].keyspace, Keyspace::JOIN_LEFT);

	assert!(tier.lookup(OP_A, &resident).is_some());
	tier.invalidate_operator(OP_A);
	assert_eq!(
		tier.keyspace_metrics().len(),
		1,
		"a purge drops resident state but not the counters, so a keyspace with history stays reported"
	);
	assert_eq!(tier.keyspace_metrics()[0].partitions, 0);
	assert_ne!(tier.keyspace_metrics()[0].counters, OperatorRangeMetrics::default());
}

#[test]
fn a_materialize_that_races_a_retraction_refuses_rather_than_reinstating_the_claim() {
	let fired = Arc::new(AtomicBool::new(false));
	let seen = fired.clone();
	let victim = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let raced = victim.clone();
	let hook: MaterializeInterlock = Box::new(move |tier: &OperatorRangeTier, _partition: PartitionId| {
		if !seen.swap(true, Ordering::Relaxed) {
			tier.mark_deleted(OP_A, &raced);
		}
	});

	let tier = OperatorRangeTier::with_interlock(
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");

	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap");

	let published = tier.materialize(&scan, &gap, &[(victim.clone(), row("stale"))]);

	assert!(fired.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert!(
		published == Materialize::Refused,
		"a claim withdrawn while the persistent read was in flight must refuse the materialize, or the materialize \
         reinstates a claim over a row the writer already removed"
	);
	assert_eq!(tier.metrics().materializes_raced, 1);
	assert_eq!(tier.metrics().materializes, 0);
	assert_eq!(
		tier.lookup(OP_A, &victim),
		None,
		"the refused materialize must roll its rows back and leave the key falling through to the store"
	);
	assert!(!covers(&tier, OP_A, &range), "and it must leave no claim behind over the span it failed to materialize");
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_concurrent_materialize_never_refuses_another_materialize() {
	let fired = Arc::new(AtomicBool::new(false));
	let seen = fired.clone();
	let hook: MaterializeInterlock = Box::new(move |tier: &OperatorRangeTier, _partition: PartitionId| {
		if !seen.swap(true, Ordering::Relaxed) {
			materialize(
				tier,
				OP_A,
				GROUP_B,
				Keyspace::ACCUMULATOR,
				&[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))],
			);
		}
	});

	let tier = OperatorRangeTier::with_interlock(
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");

	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap");
	let k = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");

	assert!(
		tier.materialize(&scan, &gap, &[(k.clone(), row("v"))]) == Materialize::Materialized,
		"only a withdrawal may refuse a materialize; the old fill handshake let two concurrent fills cancel each \
         other, which throws away work neither of them raced"
	);
	assert!(fired.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert_eq!(tier.metrics().materializes_raced, 0);
	assert_eq!(tier.lookup(OP_A, &k), Some(Some(row("v"))));
}
