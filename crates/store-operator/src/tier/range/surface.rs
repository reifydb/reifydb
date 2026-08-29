// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, KeyspaceId, OperatorStateKey, group_inner_range, keyspace_inner_range},
};
use reifydb_store::{
	coverage::{
		cursor::{RangeCursor, ServedChunk},
		interval::Interval,
		plan::{DEFAULT_GAP_GUARD, Segment},
	},
	tier::range::Materialize,
};
use reifydb_value::byte_size::ByteSize;

use super::{
	OperatorRangeConfig, OperatorRangeKeyspaceMetrics, OperatorRangeMetrics, OperatorRangeTier, PartitionId,
	RangeScan,
};

const OP_A: OperatorId = OperatorId(1);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tier(limit: u64) -> OperatorRangeTier {
	OperatorRangeTier::new(
		OperatorRangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(limit)),
			shards: 1,
			gap_guard: DEFAULT_GAP_GUARD,
		}
		.into(),
	)
	.expect("a tier with a byte budget must be constructed")
}

fn roomy() -> OperatorRangeTier {
	tier(ByteSize::from_mib(1).as_bytes())
}

fn key(group: GroupId, keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKey {
	OperatorStateKey::inner_encoded(group, keyspace, suffix).into_encoded()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn bodies(page: &[(EncodedKey, EncodedPodRow)]) -> Vec<String> {
	page.iter().map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")).collect()
}

fn materialize(
	tier: &OperatorRangeTier,
	operator: OperatorId,
	group: GroupId,
	keyspace: KeyspaceId,
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

fn one_row_partition(
	tier: &OperatorRangeTier,
	operator: OperatorId,
	group: GroupId,
	keyspace: KeyspaceId,
) -> EncodedKey {
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

#[test]
fn a_covered_span_answers_a_range_and_an_uncovered_one_falls_through() {
	let tier = roomy();
	let range = keyspace_inner_range(GROUP_A, KeyspaceId::ACCUMULATOR);

	assert!(serve_ram(&tier, OP_A, &range, 64).is_none(), "nothing is covered yet, so the read must fall through");
	assert_eq!(tier.metrics().misses, 1);

	let a = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"a");
	let b = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"b");
	materialize(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR, &[(a.clone(), row("v1")), (b.clone(), row("v2"))]);

	let served = serve_ram(&tier, OP_A, &range, 64).expect("a covered span must answer its own range");
	assert_eq!(
		bodies(&served),
		["v1", "v2"],
		"the claim must serve every row it was materialized with, in key order"
	);
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
		[b"a", b"b", b"c", b"d"].iter().map(|s| key(GROUP_A, KeyspaceId::ACCUMULATOR, *s)).collect();
	let page: Vec<(EncodedKey, EncodedPodRow)> =
		keys.iter().enumerate().map(|(index, k)| (k.clone(), row(&format!("v{index}")))).collect();
	materialize(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR, &page);

	let limited = serve_ram(&tier, OP_A, &keyspace_inner_range(GROUP_A, KeyspaceId::ACCUMULATOR), 2)
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
	let k = one_row_partition(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR);

	assert_eq!(tier.lookup(OP_A, &k), Some(Some(row("v"))), "the claim must hand back the row it holds");
	assert_eq!(tier.metrics().point_hits, 1);
	assert_eq!(tier.metrics().point_misses, 0);
}

#[test]
fn a_lookup_of_a_key_inside_a_claim_that_holds_no_row_is_a_definitive_absence() {
	let tier = roomy();
	let held = one_row_partition(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR);
	let absent = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"zzz");
	assert_ne!(held, absent);

	assert_eq!(
		tier.lookup(OP_A, &absent),
		Some(None),
		"a claim that covers the key and holds no row proves the key does not exist, and reporting a \
         fall-through instead sends every point read of an absent key to the store forever"
	);
	assert_eq!(tier.metrics().point_hits, 1, "a definitive absence is answered work, not a fall-through");
	assert_eq!(tier.metrics().point_misses, 0);

	let uncovered = key(GROUP_B, KeyspaceId::ACCUMULATOR, b"a");
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
	let k = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"a");

	assert_eq!(tier.lookup(OP_A, &k), None, "an empty tier must never answer absent for a key the store may hold");
	assert_eq!(tier.metrics().point_misses, 1);
	assert_eq!(tier.metrics().point_hits, 0);
	assert_eq!(tier.metrics().misses, 0, "a point read must not be charged to the range counters");

	materialize(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR, &[(k.clone(), row("v"))]);
	assert!(tier.lookup(OP_A, &k).is_some(), "the control: the same key answers once a scan covered it");
	assert_eq!(tier.metrics().point_misses, 1, "the materialize must not retroactively change the earlier miss");
}

#[test]
fn an_overwrite_never_creates_a_claim() {
	let tier = roomy();
	let k = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"a");

	tier.overwrite(OP_A, k.clone(), row("v"));

	assert_eq!(tier.partitions(), 0, "a write against no claim must leave the tier empty");
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.intervals(), 0, "an overwrite must never widen coverage to keys no scan observed");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a write that cached nothing must be charged nothing");
	assert_eq!(tier.lookup(OP_A, &k), None, "and the key must stay unknown rather than become a false claim");

	materialize(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR, &[]);
	tier.overwrite(OP_A, k.clone(), row("v"));
	assert_eq!(
		tier.lookup(OP_A, &k),
		Some(Some(row("v"))),
		"the control: the same write must land once a scan claimed the span it falls in"
	);
}

#[test]
fn a_materialize_keeps_a_row_already_resident_rather_than_replacing_it() {
	let tier = roomy();
	let k = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"a");
	materialize(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR, &[(k.clone(), row("v1"))]);

	let range = keyspace_inner_range(GROUP_B, KeyspaceId::ACCUMULATOR);
	let scan = tier.plan_scan(OP_A, &range).expect("an uncovered keyspace must be plannable");
	tier.overwrite(OP_A, k.clone(), row("v2"));
	let gap = first_gap(&scan).expect("the uncovered keyspace must plan as a gap");
	tier.materialize(&scan, &gap, &[(key(GROUP_B, KeyspaceId::ACCUMULATOR, b"a"), row("other"))]);

	assert_eq!(
		tier.lookup(OP_A, &k),
		Some(Some(row("v2"))),
		"a resident row is at least as new as any persistent read, so a materialize must never undo a write \
         that landed while that read was in flight"
	);
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

	let shortest_valid = key(GROUP_A, KeyspaceId::ACCUMULATOR, b"");
	assert_eq!(shortest_valid.len(), 17, "group plus keyspace with an empty suffix is the shortest valid key");
	assert!(PartitionId::of(OP_A, &shortest_valid).is_some(), "the shortest valid key must not be declined");
}

const EXCLUDED: [KeyspaceId; 1] = [KeyspaceId::CUSTOM_NOT_CACHED];

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
		tier.plan_scan(OP_A, &keyspace_inner_range(GROUP_A, KeyspaceId::ACCUMULATOR)).is_some(),
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
	assert!(OperatorRangeTier::new(
		OperatorRangeConfig {
			shard_bytes: None,
			shards: 16,
			gap_guard: DEFAULT_GAP_GUARD,
		}
		.into()
	)
	.is_none());
	assert!(OperatorRangeTier::new(OperatorRangeConfig::testing().into()).is_some());
	assert_eq!(OperatorRangeConfig::testing().shards, 2);
	assert_eq!(OperatorRangeConfig::testing().shard_bytes, Some(ByteSize::from_kib(16)));
	assert_eq!(OperatorRangeConfig::testing().gap_guard, DEFAULT_GAP_GUARD);
}

fn keyspace_row(tier: &OperatorRangeTier, keyspace: KeyspaceId) -> OperatorRangeKeyspaceMetrics {
	tier.slot_metrics()
		.into_iter()
		.find(|row| row.slot == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
}

#[test]
fn keyspace_counters_are_charged_to_the_keyspace_that_was_read() {
	let tier = roomy();
	let accumulator_range = keyspace_inner_range(GROUP_A, KeyspaceId::ACCUMULATOR);
	let buffer_range = keyspace_inner_range(GROUP_A, KeyspaceId::BUFFER);

	assert!(serve_ram(&tier, OP_A, &accumulator_range, 64).is_none());
	one_row_partition(&tier, OP_A, GROUP_A, KeyspaceId::ACCUMULATOR);
	assert!(serve_ram(&tier, OP_A, &accumulator_range, 64).is_some());
	assert!(serve_ram(&tier, OP_A, &buffer_range, 64).is_none());

	assert_eq!(
		tier.slot_metrics().len(),
		2,
		"only the two keyspaces that were touched may be reported; a fixed 256 slot table must never \
         surface as 256 rows of zeros"
	);

	let accumulator = keyspace_row(&tier, KeyspaceId::ACCUMULATOR);
	assert_eq!(accumulator.counters.hits, 1);
	assert_eq!(
		accumulator.counters.misses, 2,
		"a gap stays charged once it is handed to the store; a materialize may never take its miss back"
	);
	assert_eq!(accumulator.counters.materializes, 1);
	assert_eq!(accumulator.partitions, 1);
	assert_eq!(accumulator.entries, 1);

	let buffer = keyspace_row(&tier, KeyspaceId::BUFFER);
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
	let known = one_row_partition(&tier, OP_A, GROUP_A, KeyspaceId::EMIT);
	let unknown = key(GROUP_A, KeyspaceId::JOIN_LEFT, b"a");

	assert_eq!(tier.lookup(OP_A, &known), Some(Some(row("v"))));
	assert_eq!(tier.lookup(OP_A, &key(GROUP_A, KeyspaceId::EMIT, b"zzz")), Some(None));
	assert_eq!(tier.lookup(OP_A, &unknown), None);

	assert_eq!(keyspace_row(&tier, KeyspaceId::EMIT).counters.point_hits, 2);
	assert_eq!(keyspace_row(&tier, KeyspaceId::EMIT).counters.point_misses, 0);
	assert_eq!(keyspace_row(&tier, KeyspaceId::JOIN_LEFT).counters.point_misses, 1);
	assert_eq!(keyspace_row(&tier, KeyspaceId::JOIN_LEFT).counters.point_hits, 0);
	assert_eq!(keyspace_row(&tier, KeyspaceId::EMIT).counters.hits, 0, "a point read is not a range hit");
}

#[test]
fn keyspace_counters_are_summed_across_every_shard() {
	let tier = OperatorRangeTier::new(
		OperatorRangeConfig {
			shard_bytes: Some(ByteSize::from_mib(64)),
			shards: 4,
			gap_guard: DEFAULT_GAP_GUARD,
		}
		.into(),
	)
	.expect("a sharded tier must be constructed");

	for group in 0..64u128 {
		one_row_partition(&tier, OP_A, GroupId(group), KeyspaceId::SOURCE_WATERMARK);
	}
	for group in 0..64u128 {
		assert!(serve_ram(
			&tier,
			OP_A,
			&keyspace_inner_range(GroupId(group), KeyspaceId::SOURCE_WATERMARK),
			64
		)
		.is_some());
	}

	assert!(
		tier.shard_metrics().iter().filter(|shard| shard.counters.hits > 0).count() > 1,
		"the fixture must spread hits over more than one shard, or summation is not under test"
	);

	let reported = tier.slot_metrics();
	assert_eq!(reported.len(), 1, "one keyspace spread over four shards must collapse to a single row");
	assert_eq!(reported[0].slot, KeyspaceId::SOURCE_WATERMARK);
	assert_eq!(reported[0].counters.hits, 64);
	assert_eq!(reported[0].counters.materializes, 64);
	assert_eq!(reported[0].partitions, 64);
	assert_eq!(reported[0].entries, 64);
}

#[test]
fn a_tier_that_was_never_read_reports_no_keyspace_rows() {
	let tier = roomy();
	assert!(tier.slot_metrics().is_empty());

	let resident = one_row_partition(&tier, OP_A, GROUP_A, KeyspaceId::JOIN_LEFT);
	assert_eq!(tier.slot_metrics().len(), 1, "resident state alone must be enough to report a keyspace");
	assert_eq!(tier.slot_metrics()[0].slot, KeyspaceId::JOIN_LEFT);

	assert!(tier.lookup(OP_A, &resident).is_some());
	tier.invalidate_operator(OP_A);
	assert_eq!(
		tier.slot_metrics().len(),
		1,
		"a purge drops resident state but not the counters, so a keyspace with history stays reported"
	);
	assert_eq!(tier.slot_metrics()[0].partitions, 0);
	assert_ne!(tier.slot_metrics()[0].counters, OperatorRangeMetrics::default());
}
