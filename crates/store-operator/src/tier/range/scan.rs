// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::pod::EncodedPodRow,
	};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::state::{GroupId, KeyspaceId, OperatorStateKey, keyspace_inner_range},
			typed::ExclusiveUpperEnd,
		},
	};
	use reifydb_store::{
		coverage::{
			cursor::{RangeCursor, ServedChunk},
			interval::Interval,
			plan::Segment,
		},
		tier::range::Materialize,
	};
	use reifydb_value::byte_size::ByteSize;

	use crate::tier::range::{OperatorRangeConfig, OperatorRangeTier, PartitionId, RangeScan};

	const OP: OperatorId = OperatorId(1);
	const GROUP: GroupId = GroupId(10);
	const CACHED: KeyspaceId = KeyspaceId::ACCUMULATOR;
	const UNCACHED: KeyspaceId = KeyspaceId::CUSTOM_NOT_CACHED;

	fn tier(limit: u64, gap_guard: usize) -> OperatorRangeTier {
		OperatorRangeTier::new(OperatorRangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(limit)),
			shards: 1,
			gap_guard,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	const CACHED_ABOVE_UNCACHED: KeyspaceId = KeyspaceId(KeyspaceId::CUSTOM_NOT_CACHED.0 + 2);

	fn roomy() -> OperatorRangeTier {
		tier(ByteSize::from_mib(1).as_bytes(), 4)
	}

	fn key(keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn partition(keyspace: KeyspaceId) -> PartitionId {
		PartitionId {
			operator: OP,
			group: GROUP,
			keyspace,
		}
	}

	fn whole(keyspace: KeyspaceId) -> Interval {
		let (start, end) = partition(keyspace).span();
		Interval::new(start, end)
	}

	/// A range from the start of `top` to the end of `bottom`; keyspaces encode inverted, so `top`
	/// must be the numerically larger of the two to give an ascending key range.
	fn across(top: KeyspaceId, bottom: KeyspaceId) -> EncodedKeyRange {
		EncodedKeyRange::new(Bound::Included(key(top, b"")), keyspace_inner_range(GROUP, bottom).end)
	}

	fn claim(
		tier: &OperatorRangeTier,
		range: &EncodedKeyRange,
		span: &Interval,
		rows: &[(EncodedKey, EncodedPodRow)],
	) -> Materialize {
		let scan = tier.plan_scan(OP, range).expect("the fixture range must be plannable");
		tier.materialize(&scan, span, rows)
	}

	fn spanning(from: &EncodedKey, to: &EncodedKey) -> Interval {
		Interval::new(from.clone(), ExclusiveUpperEnd::Key(to.clone()))
	}

	fn drain(tier: &OperatorRangeTier, scan: &RangeScan, segment: &Interval, limit: usize) -> Vec<String> {
		let mut cursor = RangeCursor::new();
		let mut out = Vec::new();
		while !cursor.is_exhausted() {
			let before = out.len();
			match tier.serve(scan, segment, &mut cursor, limit) {
				ServedChunk::Served(rows) => out.extend(rows
					.into_iter()
					.map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("utf8"))),
				ServedChunk::Gap => break,
			}
			assert!(
				cursor.is_exhausted() || out.len() > before,
				"a chunk that reports more work must carry a row, or the caller's cursor never moves"
			);
		}
		out
	}

	#[test]
	fn a_plan_tiles_the_range_with_a_head_gap_a_tail_gap_and_no_merge_across_a_real_gap() {
		// Claims either side of an uncovered span must stay apart, or the hole serves as proven.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);

		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"c"), &at(b"e")),
			&[(at(b"c"), row("c")), (at(b"d"), row("d"))]
		) == Materialize::Materialized);
		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"f"), &at(b"h")),
			&[(at(b"f"), row("f")), (at(b"g"), row("g"))]
		) == Materialize::Materialized);

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		let keyspace = whole(CACHED);

		assert_eq!(
			scan.segments(),
			[
				Segment::Gap {
					interval: Interval::new(
						keyspace.start.clone(),
						ExclusiveUpperEnd::Key(at(b"c"))
					),
					exempt: false,
				},
				Segment::Resident(spanning(&at(b"c"), &at(b"e"))),
				Segment::Gap {
					interval: spanning(&at(b"e"), &at(b"f")),
					exempt: false,
				},
				Segment::Resident(spanning(&at(b"f"), &at(b"h"))),
				Segment::Gap {
					interval: Interval::new(at(b"h"), keyspace.end.clone()),
					exempt: false,
				},
			],
			"the plan must be ascending, tile the range exactly once, and keep the two claims apart"
		);
		assert_eq!(scan.gaps(), 3);
		assert!(!scan.degraded());

		assert_eq!(drain(&tier, &scan, &spanning(&at(b"c"), &at(b"e")), 64), ["c", "d"]);
		assert_eq!(drain(&tier, &scan, &spanning(&at(b"f"), &at(b"h")), 64), ["f", "g"]);
	}

	#[test]
	fn a_plan_with_more_non_exempt_gaps_than_the_guard_degrades_to_one_full_scan() {
		// A plan of many small persistent reads must be abandoned, or the caller pays a trip per hole.
		let tier = tier(ByteSize::from_mib(1).as_bytes(), 1);
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);

		assert!(claim(&tier, &range, &spanning(&at(b"b"), &at(b"c")), &[(at(b"b"), row("b"))])
			== Materialize::Materialized);
		assert!(claim(&tier, &range, &spanning(&at(b"d"), &at(b"e")), &[(at(b"d"), row("d"))])
			== Materialize::Materialized);

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		assert!(scan.degraded(), "three non-exempt gaps against a guard of one must abandon the plan");
		assert_eq!(
			scan.segments(),
			[Segment::Gap {
				interval: whole(CACHED),
				exempt: false,
			}],
			"a degraded plan must be one full scan, not the fragmented plan it replaced"
		);
	}

	#[test]
	fn a_plan_whose_excess_gaps_are_all_exempt_never_degrades() {
		// A gap that can never close must not count, or the guard trips forever on cross-keyspace reads.
		// Groups encode inverted just like keyspaces, so a range spanning two of them crosses the one
		// uncacheable keyspace once per group, giving two exempt gaps against a guard of one.
		let tier = tier(ByteSize::from_mib(1).as_bytes(), 1);
		let top = GROUP;
		let bottom = GroupId(GROUP.0 - 1);
		let span_of = |group: GroupId, keyspace: KeyspaceId| {
			PartitionId {
				operator: OP,
				group,
				keyspace,
			}
			.span()
		};
		let range = EncodedKeyRange::new(
			Bound::Included(OperatorStateKey::inner_encoded(top, UNCACHED, b"").into_encoded()),
			keyspace_inner_range(bottom, UNCACHED).end,
		);

		let upper = Interval::new(span_of(top, KeyspaceId(UNCACHED.0 - 1)).0, span_of(top, KeyspaceId(0x00)).1);
		let lower = Interval::new(
			span_of(bottom, KeyspaceId(0xff)).0,
			span_of(bottom, KeyspaceId(UNCACHED.0 + 1)).1,
		);
		assert!(
			claim(&tier, &range, &upper, &[]) == Materialize::Materialized,
			"an empty proven span is still a claim"
		);
		assert!(
			claim(&tier, &range, &lower, &[]) == Materialize::Materialized,
			"an empty proven span is still a claim"
		);

		let scan = tier.plan_scan(OP, &range).expect("a cross-group range must be plannable");
		assert_eq!(scan.gaps(), 0, "both remaining gaps lie in a keyspace that is never cached");
		assert!(
			!scan.degraded(),
			"two exempt gaps against a guard of one must not degrade, or every cross-keyspace read \
             collapses to a full scan forever"
		);
		assert_eq!(
			scan.segments()
				.iter()
				.filter(|segment| matches!(
					segment,
					Segment::Gap {
						exempt: true,
						..
					}
				))
				.count(),
			2,
			"the fixture must actually produce two exempt gaps"
		);
		assert_eq!(
			scan.segments()
				.iter()
				.filter(|segment| matches!(
					segment,
					Segment::Gap {
						exempt: false,
						..
					}
				))
				.count(),
			0,
			"the sliver at a group boundary holds no key a row could occupy, so planning a read over \
             it spends one sqlite call per boundary crossed and can never return a row"
		);
	}

	#[test]
	fn serve_never_returns_an_empty_chunk_that_reports_more_work() {
		// Only rows may count against the limit, or the cursor stays put and the scan loop spins.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);
		let span = spanning(&at(b"a"), &at(b"d"));

		assert!(claim(
			&tier,
			&range,
			&span,
			&[(at(b"a"), row("a")), (at(b"b"), row("b")), (at(b"c"), row("c"))]
		) == Materialize::Materialized);
		tier.mark_deleted(OP, &at(b"a"));
		tier.mark_deleted(OP, &at(b"b"));

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		let mut cursor = RangeCursor::new();
		let chunk = tier.serve(&scan, &span, &mut cursor, 1);
		assert_eq!(
			chunk.served().map(|rows| rows.len()),
			Some(1),
			"the two removals must be skipped, not spent against the limit"
		);
		assert!(cursor.is_exhausted(), "the segment held nothing after the row, so the chunk must be final");

		tier.mark_deleted(OP, &at(b"c"));
		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		let mut cursor = RangeCursor::new();
		let chunk = tier.serve(&scan, &span, &mut cursor, 1);
		assert_eq!(
			chunk.served(),
			Some(Vec::new()),
			"a span of nothing but removals is a proven absence, not a row"
		);
		assert!(
			cursor.is_exhausted(),
			"an empty chunk must report the segment exhausted, or the caller never advances past it"
		);
	}

	#[test]
	fn a_materialize_into_a_keyspace_that_is_never_cached_leaves_the_tier_exactly_as_it_found_it() {
		// Taking these rows would admit a keyspace the tier is configured never to hold.
		let tier = roomy();
		let range = across(UNCACHED, KeyspaceId(UNCACHED.0 - 1));
		let at = key(UNCACHED, b"a");

		assert!(
			claim(&tier, &range, &whole(UNCACHED), &[(at.clone(), row("v"))])
				== Materialize::NothingCacheable,
			"a span holding no cacheable partition must report nothing to cache, never refusal, or the \
             caller stops materializing for the rest of the scan"
		);

		assert_eq!(
			tier.partitions(),
			0,
			"an excluded keyspace must not occupy a partition, not even an empty one"
		);
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0);
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
		assert_eq!(tier.lookup(OP, &at), None);
		assert_eq!(tier.metrics().materializes, 0);
	}

	#[test]
	fn a_materialize_whose_span_crosses_a_partition_boundary_lands_rows_in_both_keyspaces() {
		// A coalesced gap hands materialize one multi-keyspace span; refusing it whole leaves cross-keyspace
		// reads permanently uncached.
		let tier = roomy();
		let top = KeyspaceId::BUFFER;
		let bottom = KeyspaceId::ACCUMULATOR;
		let head = key(top, b"a");
		let tail = key(bottom, b"m");
		let span = Interval::new(head.clone(), whole(bottom).end);
		let rows = [(head.clone(), row("top")), (tail.clone(), row("bottom"))];

		let scan = tier.plan_scan(OP, &across(top, bottom)).expect("a two-keyspace range must be plannable");
		assert!(
			tier.materialize(&scan, &span, &rows) == Materialize::Materialized,
			"a materialize spanning two cached partitions must be accepted"
		);

		let body = |key: &EncodedKey| {
			tier.lookup(OP, key)
				.map(|found| found.map(|row| String::from_utf8(row.body().to_vec()).expect("utf8")))
		};
		assert_eq!(
			body(&head),
			Some(Some("top".to_string())),
			"the partition holding the span start must keep its row"
		);
		assert_eq!(
			body(&tail),
			Some(Some("bottom".to_string())),
			"the partition past the boundary must be materialized too, not discarded with the rest of the span"
		);
	}

	#[test]
	fn a_plan_merges_contiguous_gaps_into_one_read_but_never_across_an_exempt_boundary() {
		// One read per uncovered run is the point: splitting every gap at a keyspace boundary issued a separate
		// store read per keyspace byte the scan crossed. Folding an exempt keyspace into a cached run would
		// also hide it from the gap guard.
		let tier = roomy();
		let top = KeyspaceId::BUFFER;
		let bottom = KeyspaceId::ACCUMULATOR;

		let merged = tier.plan_scan(OP, &across(top, bottom)).expect("a two-keyspace range must be plannable");
		assert_eq!(
			merged.segments(),
			[Segment::Gap {
				interval: Interval::new(whole(top).start, whole(bottom).end),
				exempt: false,
			}],
			"two adjacent uncovered cached keyspaces must read as one span, not one read per keyspace"
		);

		let split = tier
			.plan_scan(OP, &across(CACHED_ABOVE_UNCACHED, KeyspaceId::JOIN_ROW_EXPIRY))
			.expect("a range straddling an uncacheable keyspace must be plannable");
		assert_eq!(
			split.segments().len(),
			3,
			"an uncacheable keyspace must break the run, or its gap stops counting against the guard"
		);
		assert!(
			matches!(
				split.segments()[1],
				Segment::Gap {
					exempt: true,
					..
				}
			),
			"the fixture must actually straddle an exempt keyspace"
		);
	}

	#[test]
	fn a_materialize_that_proves_an_empty_span_claims_it_without_paying_for_a_partition() {
		// A span the persistent tier answered with nothing is worth claiming, but the claim lives in the
		// coverage index and the partition would hold no row. Charging one anyway lets a scan that crosses
		// many empty keyspaces spend the whole budget on structures holding nothing, which evicts the rows
		// the tier exists to serve.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);

		assert!(claim(&tier, &range, &whole(CACHED), &[]) == Materialize::Materialized);

		assert_eq!(tier.partitions(), 0, "a proof of emptiness must not materialise a partition to hold it");
		assert_eq!(tier.intervals(), 1, "the claim itself must survive, or the span is read again forever");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "an unmaterialised proof must cost no budget");
		assert_eq!(
			tier.lookup(OP, &key(CACHED, b"a")),
			Some(None),
			"the claim must still answer a point read as a proven absence"
		);
	}

	#[test]
	fn a_write_into_a_span_proved_empty_lands_instead_of_vanishing_behind_the_proof() {
		// Before, a claim implied a partition, so a write finding none could be dropped: no partition meant
		// no claim to contradict. Once a claim can outlive its partition that reasoning inverts, and a
		// dropped write leaves the claim asserting the tier holds every key in a span it no longer does.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = key(CACHED, b"a");

		assert!(claim(&tier, &range, &whole(CACHED), &[]) == Materialize::Materialized);
		assert_eq!(tier.partitions(), 0, "precondition: the claim stands with nothing behind it");

		tier.insert(OP, at.clone(), row("v"));

		assert_eq!(
			tier.lookup(OP, &at),
			Some(Some(row("v"))),
			"a write the tier swallowed while keeping the claim reads back as a proven absence, which is \
             the claim answering for a row sqlite holds"
		);
		assert_eq!(
			tier.partitions(),
			1,
			"the write is what pays for the partition, not the scan that crossed it"
		);
	}
}
