// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{keyspace::join::JoinLeft, state::GroupId},
			typed::{ExclusiveUpperEnd, Key, direction::Asc, range::KeyRange},
		},
	};
	use reifydb_store::{
		coverage::{
			cursor::{Cursor, ServedChunk},
			interval::Interval,
			plan::Segment,
		},
		tier::range::{Materialize, RangeScan, RangeTier},
	};
	use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

	use crate::tier::{
		range::{OperatorRangeConfig, tiers::RangeTiers, typed::TypedDomain},
		typed::TypedPartition,
	};

	type Suffix = Asc<RowNumber>;
	type Tier = RangeTier<TypedDomain<JoinLeft>>;

	const OP: OperatorId = OperatorId(1);
	const GROUP: GroupId = GroupId(10);

	fn tiers(limit: u64, gap_guard: usize) -> RangeTiers {
		RangeTiers::new(
			OperatorRangeConfig {
				tier_bytes: Some(ByteSize::from_bytes(limit)),
				gap_guard,
			}
			.into(),
		)
		.expect("a tier set with a byte budget must be constructed")
	}

	fn roomy() -> RangeTiers {
		tiers(ByteSize::from_mib(1).as_bytes(), 4)
	}

	fn tier(tiers: &RangeTiers) -> &Tier {
		tiers.typed::<JoinLeft>().expect("join left must own a range tier")
	}

	fn at(row: u64) -> Suffix {
		Asc(RowNumber(row))
	}

	fn part() -> TypedPartition {
		TypedPartition {
			operator: OP,
			group: GROUP,
		}
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn range() -> KeyRange<Suffix> {
		KeyRange::new(Bound::Included(Suffix::low()), Bound::Unbounded)
	}

	fn whole() -> Interval<Suffix> {
		Interval::new(Suffix::low(), ExclusiveUpperEnd::Top)
	}

	fn spanning(from: Suffix, to: Suffix) -> Interval<Suffix> {
		Interval::new(from, ExclusiveUpperEnd::Key(to))
	}

	fn plan(tiers: &RangeTiers) -> RangeScan<TypedDomain<JoinLeft>> {
		tier(tiers).plan_scan_in(part(), part(), &range()).expect("a whole-keyspace range must be plannable")
	}

	fn claim(tiers: &RangeTiers, span: &Interval<Suffix>, rows: &[(Suffix, EncodedPodRow)]) -> Materialize {
		let scan = plan(tiers);
		tier(tiers).materialize(&scan, span, rows)
	}

	fn drain(
		tiers: &RangeTiers,
		scan: &RangeScan<TypedDomain<JoinLeft>>,
		segment: &Interval<Suffix>,
		limit: usize,
	) -> Vec<String> {
		let mut cursor: Cursor<(), Suffix> = Cursor::new();
		let mut out = Vec::new();
		while !cursor.is_exhausted() {
			let before = out.len();
			match tier(tiers).serve(scan, segment, &mut cursor, limit) {
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
		let tiers = roomy();

		assert_eq!(
			claim(&tiers, &spanning(at(3), at(5)), &[(at(3), row("c")), (at(4), row("d"))]),
			Materialize::Materialized
		);
		assert_eq!(
			claim(&tiers, &spanning(at(6), at(8)), &[(at(6), row("f")), (at(7), row("g"))]),
			Materialize::Materialized
		);

		let scan = plan(&tiers);

		assert_eq!(
			scan.segments(),
			[
				Segment::Gap {
					interval: Interval::new(Suffix::low(), ExclusiveUpperEnd::Key(at(3))),
					exempt: false,
				},
				Segment::Resident(spanning(at(3), at(5))),
				Segment::Gap {
					interval: spanning(at(5), at(6)),
					exempt: false,
				},
				Segment::Resident(spanning(at(6), at(8))),
				Segment::Gap {
					interval: Interval::new(at(8), ExclusiveUpperEnd::Top),
					exempt: false,
				},
			],
			"the plan must be ascending, tile the range exactly once, and keep the two claims apart"
		);
		assert_eq!(scan.gaps(), 3);
		assert!(!scan.degraded());

		assert_eq!(drain(&tiers, &scan, &spanning(at(3), at(5)), 64), ["c", "d"]);
		assert_eq!(drain(&tiers, &scan, &spanning(at(6), at(8)), 64), ["f", "g"]);
	}

	#[test]
	fn a_plan_with_more_non_exempt_gaps_than_the_guard_degrades_to_one_full_scan() {
		// A plan of many small persistent reads must be abandoned, or the caller pays a trip per hole.
		let tiers = tiers(ByteSize::from_mib(1).as_bytes(), 1);

		assert_eq!(claim(&tiers, &spanning(at(2), at(3)), &[(at(2), row("b"))]), Materialize::Materialized);
		assert_eq!(claim(&tiers, &spanning(at(4), at(5)), &[(at(4), row("d"))]), Materialize::Materialized);

		let scan = plan(&tiers);
		assert!(scan.degraded(), "three gaps against a guard of one must abandon the plan");
		assert_eq!(
			scan.segments(),
			[Segment::Gap {
				interval: whole(),
				exempt: false,
			}],
			"a degraded plan must be one full scan, not the fragmented plan it replaced"
		);
	}

	#[test]
	fn serve_never_returns_an_empty_chunk_that_reports_more_work() {
		// Only rows may count against the limit, or the cursor stays put and the scan loop spins.
		let tiers = roomy();
		let span = spanning(at(1), at(4));

		assert!(claim(&tiers, &span, &[(at(1), row("a")), (at(2), row("b")), (at(3), row("c"))])
			== Materialize::Materialized);
		tier(&tiers).mark_deleted_in(part(), part(), &at(1));
		tier(&tiers).mark_deleted_in(part(), part(), &at(2));

		let scan = plan(&tiers);
		let mut cursor: Cursor<(), Suffix> = Cursor::new();
		let chunk = tier(&tiers).serve(&scan, &span, &mut cursor, 1);
		assert_eq!(
			chunk.served().map(|rows| rows.len()),
			Some(1),
			"the two removals must be skipped, not spent against the limit"
		);
		assert!(cursor.is_exhausted(), "the segment held nothing after the row, so the chunk must be final");

		tier(&tiers).mark_deleted_in(part(), part(), &at(3));
		let scan = plan(&tiers);
		let mut cursor: Cursor<(), Suffix> = Cursor::new();
		let chunk = tier(&tiers).serve(&scan, &span, &mut cursor, 1);
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
	fn a_materialize_lands_every_row_of_the_span_it_was_given() {
		// A gap is handed to materialize whole. Landing only the rows near the span start, or dropping the
		// tail, leaves keys inside a claimed span reading back as proven absences while sqlite still holds
		// them.
		let tiers = roomy();
		let head = at(1);
		let tail = at(9_000_000);
		let span = Interval::new(head, ExclusiveUpperEnd::Top);
		let rows = [(head, row("head")), (tail, row("tail"))];

		let scan = plan(&tiers);
		assert!(
			tier(&tiers).materialize(&scan, &span, &rows) == Materialize::Materialized,
			"a materialize over the partition's own span must be accepted"
		);

		let body = |key: &Suffix| {
			tier(&tiers)
				.lookup_in(part(), part(), key)
				.map(|found| found.map(|row| String::from_utf8(row.body().to_vec()).expect("utf8")))
		};
		assert_eq!(body(&head), Some(Some("head".to_string())), "the row at the span start must be kept");
		assert_eq!(
			body(&tail),
			Some(Some("tail".to_string())),
			"and so must the one far from it, rather than being discarded with the rest of the span"
		);
	}

	#[test]
	fn a_materialize_that_proves_an_empty_span_claims_it_without_paying_for_a_partition() {
		// A span the persistent tier answered with nothing is worth claiming, but the claim lives in the
		// coverage index and the partition would hold no row. Charging one anyway lets a scan that crosses
		// many empty spans spend the whole budget on structures holding nothing, which evicts the rows the
		// tier exists to serve.
		let tiers = roomy();

		assert_eq!(claim(&tiers, &whole(), &[]), Materialize::Materialized);

		assert_eq!(
			tier(&tiers).partitions(),
			0,
			"a proof of emptiness must not materialise a partition to hold it"
		);
		assert_eq!(
			tier(&tiers).intervals(),
			1,
			"the claim itself must survive, or the span is read again forever"
		);
		assert_eq!(
			tier(&tiers).resident_bytes(),
			ByteSize::ZERO,
			"an unmaterialised proof must cost no budget"
		);
		assert_eq!(
			tier(&tiers).lookup_in(part(), part(), &at(1)),
			Some(None),
			"the claim must still answer a point read as a proven absence"
		);
	}

	#[test]
	fn a_write_into_a_span_proved_empty_lands_instead_of_vanishing_behind_the_proof() {
		// Before, a claim implied a partition, so a write finding none could be dropped: no partition meant
		// no claim to contradict. Once a claim can outlive its partition that reasoning inverts, and a
		// dropped write leaves the claim asserting the tier holds every key in a span it no longer does.
		let tiers = roomy();

		assert_eq!(claim(&tiers, &whole(), &[]), Materialize::Materialized);
		assert_eq!(tier(&tiers).partitions(), 0, "precondition: the claim stands with nothing behind it");

		tier(&tiers).insert_in(part(), part(), at(1), row("v"));

		assert_eq!(
			tier(&tiers).lookup_in(part(), part(), &at(1)),
			Some(Some(row("v"))),
			"a write the tier swallowed while keeping the claim reads back as a proven absence, which is \
             the claim answering for a row sqlite holds"
		);
		assert_eq!(
			tier(&tiers).partitions(),
			1,
			"the write is what pays for the partition, not the scan that crossed it"
		);
	}
}
