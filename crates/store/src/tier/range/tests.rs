// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc, Barrier,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	coverage::{
		cursor::{RangeCursor, ServedChunk},
		interval::Interval,
		plan::{DEFAULT_GAP_GUARD, Segment},
	},
	tier::range::{
		ENTRY_OVERHEAD, Materialize, MaterializeInterlock, RangeConfig, RangeDomain, RangeMetrics, RangeScan,
		RangeSlotMetrics, RangeTier,
		domain::{AdmittingDomain as A, TestDomain as D, TestPartition},
		partition_overhead,
	},
};

const PARTITION_OVERHEAD: usize = partition_overhead::<D>();

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tier(limit: u64) -> RangeTier<D> {
	RangeTier::<D>::new(RangeConfig {
		shard_bytes: Some(ByteSize::from_bytes(limit)),
		shards: 1,
		gap_guard: DEFAULT_GAP_GUARD,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn roomy() -> RangeTier<D> {
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

fn per_partition_bytes() -> u64 {
	(PARTITION_OVERHEAD + footprint(&key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), &row("v"))) as u64
}

fn materialize(
	tier: &RangeTier<D>,
	operator: OperatorId,
	group: GroupId,
	keyspace: Keyspace,
	page: &[(EncodedKey, EncodedPodRow)],
) -> TestPartition {
	let range = keyspace_inner_range(group, keyspace);
	let scan = tier.plan_scan(operator, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap the fixture can materialize over");
	assert!(
		tier.materialize(&scan, &gap, page) == Materialize::Materialized,
		"the fixture page must fit the materialize it is staging"
	);
	TestPartition {
		dimension: operator,
		group,
		slot: keyspace,
	}
}

fn first_gap<X: RangeDomain>(scan: &RangeScan<X>) -> Option<Interval> {
	scan.segments().iter().find_map(|segment| match segment {
		Segment::Gap {
			interval,
			..
		} => Some(interval.clone()),
		Segment::Resident(_) => None,
	})
}

fn one_row_partition(tier: &RangeTier<D>, operator: OperatorId, group: GroupId, keyspace: Keyspace) -> EncodedKey {
	let k = key(group, keyspace, b"a");
	materialize(tier, operator, group, keyspace, &[(k.clone(), row("v"))]);
	k
}

fn serve_ram(
	tier: &RangeTier<D>,
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

fn covers(tier: &RangeTier<D>, operator: OperatorId, range: &EncodedKeyRange) -> bool {
	tier.plan_scan(operator, range)
		.map(|scan| scan.segments().iter().any(|segment| matches!(segment, Segment::Resident(_))))
		.unwrap_or(false)
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
fn a_materialize_that_does_not_fit_the_budget_is_refused_whole_and_evicts_nothing() {
	let per_partition = per_partition_bytes();
	let tier = tier(per_partition * 2);
	materialize(
		&tier,
		OP_A,
		GROUP_A,
		Keyspace::ACCUMULATOR,
		&[(key(GROUP_A, Keyspace::ACCUMULATOR, b"a"), row("v"))],
	);
	materialize(
		&tier,
		OP_A,
		GROUP_B,
		Keyspace::ACCUMULATOR,
		&[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))],
	);
	assert_eq!(tier.resident_bytes().as_bytes(), per_partition * 2, "the fixture must fill the budget exactly");
	let before = tier.resident_bytes();

	let third = GroupId(12);
	let range = keyspace_inner_range(third, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("the uncovered keyspace must plan as a gap");
	let page: Vec<(EncodedKey, EncodedPodRow)> = (0..64u8)
		.map(|index| (key(third, Keyspace::ACCUMULATOR, &[index]), row("a fairly long row body")))
		.collect();

	assert!(
		tier.materialize(&scan, &gap, &page) == Materialize::Refused,
		"a span past the shard limit must be refused"
	);

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

fn three_partition_tier() -> (RangeTier<D>, EncodedKey, EncodedKey, EncodedKey) {
	let tier = tier(per_partition_bytes() * 3);
	let touched = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let idle = key(GROUP_B, Keyspace::ACCUMULATOR, b"a");
	let grown = key(GroupId(12), Keyspace::ACCUMULATOR, b"a");
	materialize(&tier, OP_A, GROUP_A, Keyspace::ACCUMULATOR, &[(touched.clone(), row("v"))]);
	materialize(&tier, OP_A, GROUP_B, Keyspace::ACCUMULATOR, &[(idle.clone(), row("v"))]);
	materialize(&tier, OP_A, GroupId(12), Keyspace::ACCUMULATOR, &[(grown.clone(), row("v"))]);
	(tier, touched, idle, grown)
}

fn assert_idle_partition_was_the_victim(tier: &RangeTier<D>, touched: &EncodedKey, idle: &EncodedKey) {
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
	materialize(
		&tier,
		OP_A,
		GROUP_B,
		Keyspace::ACCUMULATOR,
		&[(key(GROUP_B, Keyspace::ACCUMULATOR, b"a"), row("v"))],
	);
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
fn every_shard_is_reachable_and_carries_the_configured_per_shard_budget() {
	let tier = RangeTier::<D>::new(RangeConfig {
		shard_bytes: Some(ByteSize::from_mib(64)),
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
			ByteSize::from_mib(64),
			"each shard must carry the full configured per-shard budget"
		);
	}
	assert_eq!(tier.shard_limit_bytes(), ByteSize::from_mib(64));

	for group in 0..64u128 {
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

fn keyspace_row(tier: &RangeTier<D>, keyspace: Keyspace) -> RangeSlotMetrics<D> {
	tier.slot_metrics()
		.into_iter()
		.find(|row| row.slot == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
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

	let total: u64 = tier.slot_metrics().iter().map(|row| row.used.as_bytes()).sum();
	assert_eq!(
		ByteSize::from_bytes(total),
		tier.tallied_bytes(),
		"every resident byte must be attributed to exactly one keyspace, or the table leaks or double counts"
	);
	assert_eq!(tier.slot_metrics().iter().map(|row| row.partitions).sum::<usize>(), tier.partitions());
	assert!(
		tier.slot_metrics().iter().map(|row| row.intervals).sum::<usize>() >= tier.intervals(),
		"every claim must be counted in at least one keyspace, or a fragmenting keyspace reports none"
	);
	assert_eq!(tier.slot_metrics().iter().map(|row| row.entries).sum::<usize>(), tier.entries());
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
fn a_materialize_that_races_a_retraction_refuses_rather_than_reinstating_the_claim() {
	let fired = Arc::new(AtomicBool::new(false));
	let seen = fired.clone();
	let victim = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let raced = victim.clone();
	let hook: MaterializeInterlock<D> = Box::new(move |tier: &RangeTier<D>, _partition: TestPartition| {
		if !seen.swap(true, Ordering::Relaxed) {
			tier.mark_deleted(OP_A, &raced);
		}
	});

	let tier = RangeTier::<D>::with_interlock(
		RangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
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
	assert!(
		!covers(&tier, OP_A, &range),
		"and it must leave no claim behind over the span it failed to materialize"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_refused_materialize_must_not_delete_a_row_written_while_it_was_placing() {
	let fired = Arc::new(AtomicBool::new(false));
	let seen = fired.clone();
	let contested = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let raced = contested.clone();
	let hook: MaterializeInterlock<D> = Box::new(move |tier: &RangeTier<D>, _partition: TestPartition| {
		if !seen.swap(true, Ordering::Relaxed) {
			tier.insert(OP_A, raced.clone(), row("flushed"));
			tier.record_retraction();
		}
	});

	let tier = RangeTier::<D>::with_interlock(
		RangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");

	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap");

	let published = tier.materialize(&scan, &gap, &[(contested.clone(), row("scanned"))]);

	assert!(fired.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert!(
		published == Materialize::Refused,
		"a retraction taken during the placing window must refuse the materialize"
	);
	assert_eq!(
		tier.lookup(OP_A, &contested),
		Some(Some(row("flushed"))),
		"the rollback deleted a row it never placed, so a write the store believes landed is gone"
	);
	assert!(
		!covers(&tier, OP_A, &range),
		"and the refused span must carry no claim, or the surviving row is read as proof of its neighbours"
	);
	assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
}

#[test]
fn a_concurrent_materialize_never_refuses_another_materialize() {
	let fired = Arc::new(AtomicBool::new(false));
	let seen = fired.clone();
	let hook: MaterializeInterlock<D> = Box::new(move |tier: &RangeTier<D>, _partition: TestPartition| {
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

	let tier = RangeTier::<D>::with_interlock(
		RangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
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

#[test]
fn a_materialize_places_its_rows_before_it_publishes_the_claim() {
	let fired = Arc::new(AtomicBool::new(false));
	let readable = Arc::new(AtomicBool::new(false));
	let proven_absent = Arc::new(AtomicBool::new(false));
	let (seen, saw_row, saw_proof) = (fired.clone(), readable.clone(), proven_absent.clone());
	let scanned = key(GROUP_A, Keyspace::ACCUMULATOR, b"a");
	let unscanned = key(GROUP_A, Keyspace::ACCUMULATOR, b"b");
	let (probed, absent) = (scanned.clone(), unscanned.clone());
	let hook: MaterializeInterlock<D> = Box::new(move |tier: &RangeTier<D>, _partition: TestPartition| {
		seen.store(true, Ordering::Relaxed);
		saw_row.store(tier.lookup(OP_A, &probed) == Some(Some(row("v"))), Ordering::Relaxed);
		saw_proof.store(tier.lookup(OP_A, &absent) == Some(None), Ordering::Relaxed);
	});

	let tier = RangeTier::<D>::with_interlock(
		RangeConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		},
		hook,
	)
	.expect("a tier with a byte budget must be constructed");

	let range = keyspace_inner_range(GROUP_A, Keyspace::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap");

	assert!(tier.materialize(&scan, &gap, &[(scanned.clone(), row("v"))]) == Materialize::Materialized);
	assert!(fired.load(Ordering::Relaxed), "the seam hook never fired, so the invariant went unchecked");
	assert!(
		readable.load(Ordering::Relaxed),
		"the rows a materialize scanned must be resident before its claim goes up, or a reader crossing the \
         window is told the span is proven while the rows proving it are still missing"
	);
	assert!(
		!proven_absent.load(Ordering::Relaxed),
		"a key the scan did not carry must fall through to the store until the claim is published, never answer \
         proven absent over a span the materialize has not finished"
	);
	assert_eq!(tier.lookup(OP_A, &unscanned), Some(None), "and once published the claim does prove that absence");
}

struct Lcg(u64);

impl Lcg {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		self.0 >> 33
	}
}

const SWEEP_GROUPS: u128 = 24;

const MIX: [u8; 18] = [0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4];
const SWEEP_KEYS: u64 = 8;

fn sweep_key(group: u128, n: u64) -> EncodedKey {
	key(GroupId(group), Keyspace::ACCUMULATOR, format!("k{n}").as_bytes())
}

fn sweep_domain() -> Vec<EncodedKey> {
	(1..=SWEEP_GROUPS).flat_map(|group| (0..SWEEP_KEYS).map(move |n| sweep_key(group, n))).collect()
}

fn sweep_page(group: u128) -> Vec<(EncodedKey, EncodedPodRow)> {
	(0..SWEEP_KEYS).map(|n| (sweep_key(group, n), row("m"))).collect()
}

trait Sweep: RangeDomain<Dimension = OperatorId, Partition = TestPartition, Row = EncodedPodRow> {}

impl Sweep for D {}

impl Sweep for A {}

fn sweep_tier<X: Sweep>(budget: u64) -> RangeTier<X> {
	RangeTier::<X>::new(RangeConfig {
		shard_bytes: Some(ByteSize::from_bytes(budget)),
		shards: 1,
		gap_guard: DEFAULT_GAP_GUARD,
	})
	.expect("a tier with a byte budget must be constructed")
}

fn sweep_materialize<X: Sweep>(tier: &RangeTier<X>, group: u128) {
	let range = keyspace_inner_range(GroupId(group), Keyspace::ACCUMULATOR);
	let Some(scan) = tier.plan_scan(OP_A, &range) else {
		return;
	};
	let Some(gap) = first_gap(&scan) else {
		return;
	};
	let page: Vec<_> = sweep_page(group).into_iter().filter(|(key, _)| gap.contains(key)).collect();
	tier.materialize(&scan, &gap, &page);
}

fn sweep_explain<X: Sweep>(tier: &RangeTier<X>, key: &EncodedKey, at: usize) -> String {
	let partition = TestPartition::of(OP_A, key).expect("a domain key must map to a partition");
	let shard = tier.shard(tier.shard_index(&partition)).lock();
	let resident = shard.partitions.get(&partition);
	format!(
		"key[{at}] group={} partition_resident={} covered={:?} entries={:?} holds_key={:?} claimed={}",
		partition.group.0,
		resident.is_some(),
		resident.map(|target| target.covered),
		resident.map(|target| target.entries.len()),
		resident.map(|target| target.entries.contains_key(key)),
		tier.coverage().read().contains(OP_A, key),
	)
}

fn sweep_step<X: Sweep>(tier: &RangeTier<X>, rng: &mut Lcg, domain: &[EncodedKey]) {
	let at = domain[(rng.next() % domain.len() as u64) as usize].clone();
	match MIX[(rng.next() % MIX.len() as u64) as usize] {
		0 => tier.insert(OP_A, at, row("i")),
		1 => sweep_materialize(tier, rng.next() as u128 % SWEEP_GROUPS + 1),
		2 => tier.invalidate(OP_A, &at),
		3 => tier.evict_to_capacity(0),
		_ => tier.clear(),
	}
}

fn sweep<X: Sweep>() -> RangeMetrics {
	const THREADS: usize = 4;
	const ROUNDS: usize = 200;
	const STEPS: usize = 5;
	const SEEDS: [u64; 4] = [1, 29, 307, 4517];

	let domain = sweep_domain();
	let per_key = footprint(&sweep_key(1, 0), &row("m"));
	let budget = 2 * (PARTITION_OVERHEAD + SWEEP_KEYS as usize * per_key) as u64;
	let mut total = RangeMetrics::default();

	for seed in SEEDS {
		let tier = Arc::new(sweep_tier::<X>(budget));
		let barrier = Arc::new(Barrier::new(THREADS));
		let overstated = Arc::new(AtomicUsize::new(usize::MAX));
		let mut handles = Vec::with_capacity(THREADS);

		for id in 0..THREADS {
			let tier = tier.clone();
			let barrier = barrier.clone();
			let overstated = overstated.clone();
			let domain = domain.clone();
			handles.push(thread::spawn(move || {
				let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(id as u64));
				for _ in 0..ROUNDS {
					for _ in 0..STEPS {
						sweep_step(&tier, &mut rng, &domain);
					}
					barrier.wait();
					if id == 0 {
						let found = domain
							.iter()
							.position(|key| tier.lookup(OP_A, key) == Some(None));
						if let Some(at) = found {
							let _ = overstated.compare_exchange(
								usize::MAX,
								at,
								Ordering::SeqCst,
								Ordering::SeqCst,
							);
						}
					}
					barrier.wait();
				}
			}));
		}
		for handle in handles {
			handle.join().expect("a sweep thread must not panic");
		}

		let metrics = tier.metrics();
		total.materializes += metrics.materializes;
		total.materializes_refused += metrics.materializes_refused;
		total.materializes_raced += metrics.materializes_raced;
		total.evictions += metrics.evictions;

		let at = overstated.load(Ordering::SeqCst);
		assert_eq!(
			at,
			usize::MAX,
			"seed {seed}: coverage proved a key absent that holds a value: {}",
			sweep_explain(&tier, &domain[at], at)
		);
	}

	total
}

#[test]
fn a_handoff_domain_never_overstates_coverage_under_concurrent_writes_and_invalidates() {
	let total = sweep::<D>();

	assert!(
		total.materializes > 100,
		"only {} spans claimed: nothing was ever claimed to overstate",
		total.materializes
	);
	assert!(
		total.materializes_raced > 0,
		"no materialize was refused by a token, so the claim-versus-shrink race never ran"
	);
	assert_eq!(
		total.evictions, 0,
		"a materialize that does not fit is refused whole, so nothing on this domain can push the budget over"
	);
}

#[test]
fn an_admitting_domain_never_overstates_coverage_under_concurrent_writes_and_evictions() {
	let total = sweep::<A>();

	assert!(
		total.materializes > 100,
		"only {} spans claimed: nothing was ever claimed to overstate",
		total.materializes
	);
	assert!(
		total.evictions > 100,
		"only {} evictions: the byte budget never forced the retraction path",
		total.evictions
	);
	assert!(
		total.materializes_raced > 0,
		"no materialize was refused by a token, so the claim-versus-shrink race never ran"
	);
}
