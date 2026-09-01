// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{
				join::{JoinLeft, JoinRight},
				root::CustomNotCached,
			},
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{Key, direction::Asc, range::KeyRange},
	},
};
use reifydb_store::{
	coverage::{
		cursor::{Cursor, ServedChunk},
		interval::Interval,
		plan::{DEFAULT_GAP_GUARD, Segment},
	},
	tier::range::{Materialize, RangeScan, RangeTier},
};
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use super::{
	OperatorRangeConfig, OperatorRangeMetrics,
	tiers::{OperatorRangeKeyspaceMetrics, RangeTiers},
	typed::TypedDomain,
};
use crate::tier::typed::TypedPartition;

const OP_A: OperatorId = OperatorId(1);
const GROUP_A: GroupId = GroupId(10);
const GROUP_B: GroupId = GroupId(11);

fn tiers(limit: u64) -> RangeTiers {
	RangeTiers::new(
		OperatorRangeConfig {
			tier_bytes: Some(ByteSize::from_bytes(limit)),
			gap_guard: DEFAULT_GAP_GUARD,
		}
		.into(),
	)
	.expect("a tier set with a byte budget must be constructed")
}

fn roomy() -> RangeTiers {
	tiers(ByteSize::from_mib(1).as_bytes())
}

fn tier_of<K: Keyspace>(tiers: &RangeTiers) -> &RangeTier<TypedDomain<K>> {
	tiers.typed::<K>().unwrap_or_else(|| panic!("{} must own a range tier", K::NAME))
}

fn at(row: u64) -> Asc<RowNumber> {
	Asc(RowNumber(row))
}

fn part(group: GroupId) -> TypedPartition {
	TypedPartition {
		operator: OP_A,
		group,
	}
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn bodies<K>(page: &[(K, EncodedPodRow)]) -> Vec<String> {
	page.iter().map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")).collect()
}

fn whole() -> KeyRange<Asc<RowNumber>> {
	KeyRange::new(Bound::Included(Asc::<RowNumber>::low()), Bound::Unbounded)
}

fn plan<K: Keyspace<Suffix = Asc<RowNumber>>>(
	tiers: &RangeTiers,
	group: GroupId,
	range: &KeyRange<Asc<RowNumber>>,
) -> Option<RangeScan<TypedDomain<K>>> {
	tier_of::<K>(tiers).plan_scan_in(part(group), part(group), range)
}

fn first_gap<K: Keyspace>(scan: &RangeScan<TypedDomain<K>>) -> Option<Interval<K::Suffix>> {
	scan.segments().iter().find_map(|segment| match segment {
		Segment::Gap {
			interval,
			..
		} => Some(interval.clone()),
		Segment::Resident(_) => None,
	})
}

fn materialize<K: Keyspace<Suffix = Asc<RowNumber>>>(
	tiers: &RangeTiers,
	group: GroupId,
	page: &[(Asc<RowNumber>, EncodedPodRow)],
) {
	let tier = tier_of::<K>(tiers);
	let scan = plan::<K>(tiers, group, &whole()).expect("a whole-keyspace range must be plannable");
	let gap = first_gap(&scan).expect("an uncovered keyspace must plan as a gap the fixture can materialize over");
	assert!(
		tier.materialize(&scan, &gap, page) == Materialize::Materialized,
		"the fixture page must fit the materialize it is staging"
	);
}

fn one_row_partition<K: Keyspace<Suffix = Asc<RowNumber>>>(tiers: &RangeTiers, group: GroupId) -> Asc<RowNumber> {
	let k = at(1);
	materialize::<K>(tiers, group, &[(k, row("v"))]);
	k
}

fn serve_ram<K: Keyspace<Suffix = Asc<RowNumber>>>(
	tiers: &RangeTiers,
	group: GroupId,
	range: &KeyRange<Asc<RowNumber>>,
	limit: usize,
) -> Option<Vec<(Asc<RowNumber>, EncodedPodRow)>> {
	let tier = tier_of::<K>(tiers);
	let scan = plan::<K>(tiers, group, range)?;
	let mut out: Vec<(Asc<RowNumber>, EncodedPodRow)> = Vec::new();
	let mut resident = false;

	for segment in scan.segments() {
		let Segment::Resident(interval) = segment else {
			continue;
		};
		resident = true;
		let mut cursor: Cursor<(), Asc<RowNumber>> = Cursor::new();
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
	let tiers = roomy();

	assert!(
		serve_ram::<JoinLeft>(&tiers, GROUP_A, &whole(), 64).is_none(),
		"nothing is covered yet, so the read must fall through"
	);
	assert_eq!(tiers.metrics().misses, 1);

	let a = at(1);
	let b = at(2);
	materialize::<JoinLeft>(&tiers, GROUP_A, &[(a, row("v1")), (b, row("v2"))]);

	let served =
		serve_ram::<JoinLeft>(&tiers, GROUP_A, &whole(), 64).expect("a covered span must answer its own range");
	assert_eq!(
		bodies(&served),
		["v1", "v2"],
		"the claim must serve every row it was materialized with, in key order"
	);
	assert_eq!(served[0].0, a);
	assert_eq!(served[1].0, b);
	assert_eq!(tiers.metrics().hits, 1);
	assert_eq!(
		tiers.metrics().misses,
		2,
		"the fixture plans twice, so both gaps are charged; a hit must add a hit and rescind nothing"
	);
}

#[test]
fn a_range_serves_only_the_slice_it_was_asked_for() {
	let tiers = roomy();
	let keys: Vec<Asc<RowNumber>> = (1..=4).map(at).collect();
	let page: Vec<(Asc<RowNumber>, EncodedPodRow)> =
		keys.iter().enumerate().map(|(index, k)| (*k, row(&format!("v{index}")))).collect();
	materialize::<JoinLeft>(&tiers, GROUP_A, &page);

	let limited = serve_ram::<JoinLeft>(&tiers, GROUP_A, &whole(), 2).expect("the covered span must answer");
	assert_eq!(
		bodies(&limited),
		["v0", "v1"],
		"the limit must truncate from the start of the range, not be ignored"
	);

	let sub = serve_ram::<JoinLeft>(
		&tiers,
		GROUP_A,
		&KeyRange::new(Bound::Included(keys[1]), Bound::Excluded(keys[3])),
		64,
	)
	.expect("a sub-range of a covered span must be answerable from it");
	assert_eq!(bodies(&sub), ["v1", "v2"], "an excluded end must stay excluded and an included start included");

	let empty = serve_ram::<JoinLeft>(
		&tiers,
		GROUP_A,
		&KeyRange::new(Bound::Excluded(keys[1]), Bound::Excluded(keys[1])),
		64,
	)
	.expect("a degenerate range over a covered span is still answered by it");
	assert!(empty.is_empty(), "a range that excludes both ends of one key selects nothing");
}

#[test]
fn a_lookup_of_a_key_the_claim_holds_serves_the_row() {
	let tiers = roomy();
	let k = one_row_partition::<JoinLeft>(&tiers, GROUP_A);

	assert_eq!(
		tier_of::<JoinLeft>(&tiers).lookup_in(part(GROUP_A), part(GROUP_A), &k),
		Some(Some(row("v"))),
		"the claim must hand back the row it holds"
	);
	assert_eq!(tiers.metrics().point_hits, 1);
	assert_eq!(tiers.metrics().point_misses, 0);
}

#[test]
fn a_lookup_of_a_key_inside_a_claim_that_holds_no_row_is_a_definitive_absence() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let held = one_row_partition::<JoinLeft>(&tiers, GROUP_A);
	let absent = at(999);
	assert_ne!(held, absent);

	assert_eq!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &absent),
		Some(None),
		"a claim that covers the key and holds no row proves the key does not exist, and reporting a \
         fall-through instead sends every point read of an absent key to the store forever"
	);
	assert_eq!(tiers.metrics().point_hits, 1, "a definitive absence is answered work, not a fall-through");
	assert_eq!(tiers.metrics().point_misses, 0);

	assert_eq!(
		tier.lookup_in(part(GROUP_B), part(GROUP_B), &at(1)),
		None,
		"the answer is only definitive inside a span some scan actually proved, never outside it"
	);
	assert_eq!(tiers.metrics().point_misses, 1);
}

#[test]
fn a_lookup_with_nothing_covered_falls_through_and_charges_a_point_miss() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = at(1);

	assert_eq!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &k),
		None,
		"an empty tier must never answer absent for a key the store may hold"
	);
	assert_eq!(tiers.metrics().point_misses, 1);
	assert_eq!(tiers.metrics().point_hits, 0);
	assert_eq!(tiers.metrics().misses, 0, "a point read must not be charged to the range counters");

	materialize::<JoinLeft>(&tiers, GROUP_A, &[(k, row("v"))]);
	assert!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &k).is_some(),
		"the control: the same key answers once a scan covered it"
	);
	assert_eq!(tiers.metrics().point_misses, 1, "the materialize must not retroactively change the earlier miss");
}

#[test]
fn an_overwrite_never_creates_a_claim() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = at(1);

	tier.overwrite_in(part(GROUP_A), part(GROUP_A), k, row("v"));

	assert_eq!(tier.partitions(), 0, "a write against no claim must leave the tier empty");
	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.intervals(), 0, "an overwrite must never widen coverage to keys no scan observed");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a write that cached nothing must be charged nothing");
	assert_eq!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &k),
		None,
		"and the key must stay unknown rather than become a false claim"
	);

	materialize::<JoinLeft>(&tiers, GROUP_A, &[]);
	tier.overwrite_in(part(GROUP_A), part(GROUP_A), k, row("v"));
	assert_eq!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &k),
		Some(Some(row("v"))),
		"the control: the same write must land once a scan claimed the span it falls in"
	);
}

#[test]
fn a_materialize_keeps_a_row_already_resident_rather_than_replacing_it() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = at(1);
	materialize::<JoinLeft>(&tiers, GROUP_A, &[(k, row("v1"))]);

	let scan = plan::<JoinLeft>(&tiers, GROUP_B, &whole()).expect("an uncovered group must be plannable");
	tier.overwrite_in(part(GROUP_A), part(GROUP_A), k, row("v2"));
	let gap = first_gap(&scan).expect("the uncovered group must plan as a gap");
	tier.materialize(&scan, &gap, &[(at(1), row("other"))]);

	assert_eq!(
		tier.lookup_in(part(GROUP_A), part(GROUP_A), &k),
		Some(Some(row("v2"))),
		"a resident row is at least as new as any persistent read, so a materialize must never undo a write \
         that landed while that read was in flight"
	);
}

#[test]
fn a_keyspace_that_caches_no_ranges_owns_no_tier_at_all() {
	let tiers = roomy();

	assert!(
		tiers.typed::<CustomNotCached>().is_none(),
		"{} caches nothing, so it must never be given a tier; a tier that exists only to refuse every call \
         still costs a budget, a lock and a metrics row",
		CustomNotCached::NAME
	);
	assert!(
		tiers.of(KeyspaceId::CUSTOM_NOT_CACHED).is_none(),
		"and the runtime lookup must agree with the typed one, or a byte-keyed caller admits what the typed \
         caller refuses"
	);
	assert!(
		tiers.typed::<JoinLeft>().is_some(),
		"the control: a gate that refused every keyspace would pass the assertions above while turning the \
         whole tier set into an off switch, and that only shows up as a throughput loss in a replay"
	);
}

#[test]
fn a_tier_without_a_byte_budget_is_not_constructed() {
	assert!(RangeTiers::new(
		OperatorRangeConfig {
			tier_bytes: None,
			gap_guard: DEFAULT_GAP_GUARD,
		}
		.into()
	)
	.is_none());
	assert!(RangeTiers::new(OperatorRangeConfig::testing().into()).is_some());
	assert_eq!(OperatorRangeConfig::testing().tier_bytes, Some(ByteSize::from_kib(32)));
	assert_eq!(OperatorRangeConfig::testing().gap_guard, DEFAULT_GAP_GUARD);
}

fn keyspace_row(tiers: &RangeTiers, keyspace: KeyspaceId) -> OperatorRangeKeyspaceMetrics {
	tiers.keyspace_metrics()
		.into_iter()
		.find(|row| row.bucket == keyspace)
		.unwrap_or_else(|| panic!("keyspace {} must be reported", keyspace.name()))
}

#[test]
fn keyspace_counters_are_charged_to_the_keyspace_that_was_read() {
	let tiers = roomy();

	assert!(serve_ram::<JoinLeft>(&tiers, GROUP_A, &whole(), 64).is_none());
	one_row_partition::<JoinLeft>(&tiers, GROUP_A);
	assert!(serve_ram::<JoinLeft>(&tiers, GROUP_A, &whole(), 64).is_some());
	assert!(serve_ram::<JoinRight>(&tiers, GROUP_A, &whole(), 64).is_none());

	assert_eq!(
		tiers.keyspace_metrics().len(),
		2,
		"only the two keyspaces that were touched may be reported; a tier per keyspace must never surface as \
         42 rows of zeros"
	);

	let left = keyspace_row(&tiers, KeyspaceId::JOIN_LEFT);
	assert_eq!(left.counters.hits, 1);
	assert_eq!(
		left.counters.misses, 2,
		"a gap stays charged once it is handed to the store; a materialize may never take its miss back"
	);
	assert_eq!(left.counters.materializes, 1);
	assert_eq!(left.partitions, 1);
	assert_eq!(left.entries, 1);

	let right = keyspace_row(&tiers, KeyspaceId::JOIN_RIGHT);
	assert_eq!(right.counters.hits, 0, "a miss in one keyspace must not borrow the other keyspace's hit");
	assert_eq!(right.counters.misses, 1);
	assert_eq!(right.partitions, 0, "a keyspace with no resident partition is still reported once a counter moved");

	assert_eq!(tiers.metrics().hits, 1, "the aggregate must survive alongside the keyspace table");
	assert_eq!(
		tiers.metrics().misses,
		3,
		"the aggregate is the sum of the keyspace rows: two join left gaps and one join right gap"
	);
}

#[test]
fn point_counters_are_charged_to_the_keyspace_that_was_looked_up() {
	let tiers = roomy();
	let known = one_row_partition::<JoinLeft>(&tiers, GROUP_A);

	assert_eq!(tier_of::<JoinLeft>(&tiers).lookup_in(part(GROUP_A), part(GROUP_A), &known), Some(Some(row("v"))));
	assert_eq!(tier_of::<JoinLeft>(&tiers).lookup_in(part(GROUP_A), part(GROUP_A), &at(999)), Some(None));
	assert_eq!(tier_of::<JoinRight>(&tiers).lookup_in(part(GROUP_A), part(GROUP_A), &at(1)), None);

	assert_eq!(keyspace_row(&tiers, KeyspaceId::JOIN_LEFT).counters.point_hits, 2);
	assert_eq!(keyspace_row(&tiers, KeyspaceId::JOIN_LEFT).counters.point_misses, 0);
	assert_eq!(keyspace_row(&tiers, KeyspaceId::JOIN_RIGHT).counters.point_misses, 1);
	assert_eq!(keyspace_row(&tiers, KeyspaceId::JOIN_RIGHT).counters.point_hits, 0);
	assert_eq!(keyspace_row(&tiers, KeyspaceId::JOIN_LEFT).counters.hits, 0, "a point read is not a range hit");
}

#[test]
fn a_tier_that_was_never_read_reports_no_keyspace_rows() {
	let tiers = roomy();
	assert!(
		tiers.keyspace_metrics().is_empty(),
		"every keyspace owns a tier from construction, but an untouched tier must still report nothing"
	);

	let resident = one_row_partition::<JoinLeft>(&tiers, GROUP_A);
	assert_eq!(tiers.keyspace_metrics().len(), 1, "resident state alone must be enough to report a keyspace");
	assert_eq!(tiers.keyspace_metrics()[0].bucket, KeyspaceId::JOIN_LEFT);

	let tier = tier_of::<JoinLeft>(&tiers);
	assert!(tier.lookup_in(part(GROUP_A), part(GROUP_A), &resident).is_some());
	tier.invalidate_dimensions_where(|dimension| dimension.operator == OP_A);
	assert_eq!(
		tiers.keyspace_metrics().len(),
		1,
		"a purge drops resident state but not the counters, so a keyspace with history stays reported"
	);
	assert_eq!(tiers.keyspace_metrics()[0].partitions, 0);
	assert_ne!(tiers.keyspace_metrics()[0].counters, OperatorRangeMetrics::default());
}

const OP_B: OperatorId = OperatorId(2);

fn keys<K: Keyspace<Suffix = Asc<RowNumber>>>(
	tiers: &RangeTiers,
	operator: OperatorId,
	group: GroupId,
) -> Option<Vec<Asc<RowNumber>>> {
	let tier = tier_of::<K>(tiers);
	let at = TypedPartition {
		operator,
		group,
	};
	let scan = tier.plan_scan_in(at, at, &whole())?;
	let mut out: Vec<Asc<RowNumber>> = Vec::new();
	let mut resident = false;
	for segment in scan.segments() {
		let Segment::Resident(interval) = segment else {
			continue;
		};
		resident = true;
		let mut cursor: Cursor<(), Asc<RowNumber>> = Cursor::new();
		while !cursor.is_exhausted() {
			match tier.serve(&scan, interval, &mut cursor, 64) {
				ServedChunk::Served(rows) => out.extend(rows.into_iter().map(|(key, _)| key)),
				ServedChunk::Gap => break,
			}
		}
	}
	resident.then_some(out)
}

#[test]
fn a_claim_and_a_serve_round_trip_for_the_operator_that_made_it() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = one_row_partition::<JoinLeft>(&tiers, GROUP_A);

	assert_eq!(keys::<JoinLeft>(&tiers, OP_A, GROUP_A), Some(vec![k]));
	assert_eq!(tier.lookup_in(part(GROUP_A), part(GROUP_A), &k), Some(Some(row("v"))));

	assert_eq!(keys::<JoinLeft>(&tiers, OP_B, GROUP_A), None);
	let other = TypedPartition {
		operator: OP_B,
		group: GROUP_A,
	};
	assert_eq!(tier.lookup_in(other, other, &k), None);
}

#[test]
fn invalidating_an_operator_withdraws_the_claim_it_made() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = one_row_partition::<JoinLeft>(&tiers, GROUP_A);
	assert_eq!(tier.lookup_in(part(GROUP_A), part(GROUP_A), &k), Some(Some(row("v"))));

	tiers.invalidate_operator(OP_A);

	assert_eq!(tier.entries(), 0);
	assert_eq!(tier.intervals(), 0);
	assert_eq!(tier.lookup_in(part(GROUP_A), part(GROUP_A), &k), None);
	assert_eq!(keys::<JoinLeft>(&tiers, OP_A, GROUP_A), None);
}

#[test]
fn invalidating_one_operator_leaves_every_group_of_every_other_operator_claimed() {
	let tiers = roomy();
	let tier = tier_of::<JoinLeft>(&tiers);
	let k = at(1);

	for (operator, group) in [(OP_A, GROUP_A), (OP_A, GROUP_B), (OP_B, GROUP_A)] {
		let held = TypedPartition {
			operator,
			group,
		};
		let scan = tier.plan_scan_in(held, held, &whole()).expect("an uncovered group must be plannable");
		let gap = first_gap(&scan).expect("an uncovered group must plan as a gap");
		assert_eq!(tier.materialize(&scan, &gap, &[(k, row("v"))]), Materialize::Materialized);
	}
	assert_eq!(tier.partitions(), 3, "the fixture must stage three independent claims");

	tiers.invalidate_operator(OP_A);

	assert_eq!(tier.lookup_in(part(GROUP_A), part(GROUP_A), &k), None, "the purged operator's first group");
	assert_eq!(tier.lookup_in(part(GROUP_B), part(GROUP_B), &k), None, "and every other group it claimed");
	let survivor = TypedPartition {
		operator: OP_B,
		group: GROUP_A,
	};
	assert_eq!(
		tier.lookup_in(survivor, survivor, &k),
		Some(Some(row("v"))),
		"another operator's claim in the same group must survive, or one flow restarting cold-starts the rest"
	);
	assert_eq!(tier.partitions(), 1);
}
