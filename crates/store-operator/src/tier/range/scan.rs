// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	ops::Bound::{Excluded, Included, Unbounded},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_store::coverage::{
	CoverageSet, Edge, Entry, Interval, PinnedCount, RangeCursor, Residency, Segment, ServedChunk, plan, successor,
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::range::{
	Install, OperatorRangeTier, PARTITION_OVERHEAD, Partition, PartitionId, RangeRows, RangeScan, Shard,
	entry_footprint,
};

impl RangeScan {
	pub fn segments(&self) -> &[Segment] {
		&self.segments
	}

	pub fn gaps(&self) -> usize {
		self.gaps
	}

	pub fn degraded(&self) -> bool {
		self.degraded
	}

	pub fn operator(&self) -> OperatorId {
		self.operator
	}
}

impl OperatorRangeTier {
	pub fn plan_scan(&self, operator: OperatorId, range: &EncodedKeyRange) -> Option<RangeScan> {
		let lo = match range.start.as_ref() {
			Included(key) => key.clone(),
			Excluded(key) => successor(key),
			Unbounded => return None,
		};
		let hi = match range.end.as_ref() {
			Included(key) => Edge::Key(successor(key)),
			Excluded(key) => Edge::Key(key.clone()),
			Unbounded => Edge::Top,
		};

		let head = partition_at(operator, &lo)?;
		let (_, head_end) = head.span();
		if hi <= head_end && !head.caches_ranges() {
			return None;
		}

		let (planned, held, retractions) = {
			let coverage = self.coverage().read();
			let vacant = CoverageSet::new();
			let claims = coverage.operators.get(&operator).unwrap_or(&vacant);
			let planned =
				plan(claims, lo.clone(), hi.clone(), self.gap_guard(), |gap| exempt_gap(operator, gap));
			let held = claims.contains(&lo);
			(planned, held, self.retractions())
		};

		let mut pieces = Vec::with_capacity(planned.segments.len().max(1));
		if planned.segments.is_empty() {
			let interval = Interval::new(lo, hi);
			pieces.push((
				if held {
					Segment::Ram(interval)
				} else {
					Segment::Gap {
						interval,
						exempt: !head.caches_ranges(),
					}
				},
				Some(head),
			));
		}
		for segment in &planned.segments {
			split_at_partitions(operator, segment, &mut pieces);
		}
		let pieces = coalesce_gaps(pieces);
		if pieces.is_empty() {
			return Some(RangeScan {
				operator,
				segments: Vec::new(),
				gaps: planned.gaps - planned.exempt_gaps,
				degraded: planned.degraded,
				retractions,
			});
		}

		let head_shard = self.shard_index(&head);
		let mut orphaned = 0;
		let mut work = Vec::with_capacity(pieces.len());
		for (segment, partition) in &pieces {
			let tally = match segment {
				Segment::Ram(_) => Tally::Hit,
				Segment::Gap {
					exempt: true,
					..
				} => Tally::Untallied,
				Segment::Gap {
					exempt: false,
					..
				} => Tally::Miss,
			};
			match partition {
				Some(partition) => work.push((self.shard_index(partition), Some(*partition), tally)),
				None => {
					if matches!(tally, Tally::Miss) {
						orphaned += 1;
					}
				}
			}
		}
		work.sort_by_key(|(index, _, _)| *index);
		if orphaned > 0 {
			work.push((head_shard, None, Tally::Untallied));
		}

		let mut at = 0;
		let mut recorded = false;
		while at < work.len() {
			let index = work[at].0;
			let mut shard = self.shard(index).lock();
			if index == head_shard && !recorded {
				recorded = true;
				shard.gaps.record(&planned);
				shard.metrics.misses += orphaned;
			}
			while at < work.len() && work[at].0 == index {
				let (_, partition, tally) = work[at];
				at += 1;
				let Some(partition) = partition else {
					continue;
				};
				let slot = partition.keyspace.0 as usize;
				match tally {
					Tally::Hit => {
						shard.metrics.hits += 1;
						shard.keyspace_metrics[slot].hits += 1;
					}
					Tally::Miss => {
						shard.metrics.misses += 1;
						shard.keyspace_metrics[slot].misses += 1;
					}
					Tally::Untallied => {}
				}
				if partition.caches_ranges() {
					if let Some(resident) = shard.partitions.get_mut(&partition) {
						resident.covered = true;
					}
				}
			}
		}

		Some(RangeScan {
			operator,
			segments: pieces.into_iter().map(|(segment, _)| segment).collect(),
			gaps: planned.gaps - planned.exempt_gaps,
			degraded: planned.degraded,
			retractions,
		})
	}

	pub fn serve(
		&self,
		scan: &RangeScan,
		segment: &Interval,
		cursor: &mut RangeCursor,
		limit: usize,
	) -> ServedChunk<RangeRows> {
		let start = match cursor.last_key.as_ref() {
			Some(last) if last.as_slice() >= segment.start.as_slice() => successor(last),
			_ => segment.start.clone(),
		};
		if !segment.end.covers(&start) {
			cursor.finish();
			return ServedChunk::Served(Vec::new());
		}

		let Some(partition) = PartitionId::of(scan.operator, &segment.start) else {
			return ServedChunk::Gap;
		};
		if !partition.caches_ranges() {
			return ServedChunk::Gap;
		}

		let observed = {
			let coverage = self.coverage().read();
			let Some(claims) = coverage.operators.get(&scan.operator) else {
				return ServedChunk::Gap;
			};
			match claims.covering(&start) {
				Some(claim) if claim.end >= segment.end => self.retractions(),
				_ => return ServedChunk::Gap,
			}
		};

		let limit = limit.max(1);
		let mut rows: RangeRows = Vec::new();
		let mut exhausted = true;
		{
			let mut shard = self.shard_for(&partition).lock();
			let tick = shard.next_tick;
			{
				let Shard {
					partitions,
					..
				} = &mut *shard;
				if let Some(resident) = partitions.get_mut(&partition) {
					resident.tick = tick;
					let upper = match &segment.end {
						Edge::Key(key) => Excluded(key.clone()),
						Edge::Top => Unbounded,
					};
					let span = (Included(start), upper);
					for (key, entry) in resident.entries.range::<EncodedKey, _>(span) {
						let Some(row) = entry.value() else {
							continue;
						};
						if rows.len() == limit {
							exhausted = false;
							break;
						}
						rows.push((key.clone(), row.clone()));
					}
				}
			}
			shard.next_tick = tick + 1;
		}

		if self.retractions() != observed {
			return ServedChunk::Gap;
		}

		assert!(
			exhausted || !rows.is_empty(),
			"a chunk that does not exhaust its segment must carry a row, or the scan loop never advances"
		);

		if let Some((key, _)) = rows.last() {
			cursor.advance(key.clone());
		}
		if exhausted {
			cursor.finish();
		}
		ServedChunk::Served(rows)
	}

	pub fn install(&self, scan: &RangeScan, span: &Interval, rows: &[(EncodedKey, EncodedPodRow)]) -> Install {
		let mut start = span.start.clone();
		let mut installed = false;
		if !span.is_empty() {
			loop {
				let Some(partition) = PartitionId::of(scan.operator, &start) else {
					break;
				};
				let (_, bound) = partition.span();
				let end = bound.min(span.end.clone());
				let piece = Interval::new(start, end.clone());
				if partition.caches_ranges() && !piece.is_empty() {
					if !self.install_partition(scan, &piece, rows) {
						return Install::Refused;
					}
					installed = true;
				}
				if end == span.end {
					break;
				}
				match end {
					Edge::Key(key) => start = key,
					Edge::Top => break,
				}
			}
		}
		if installed {
			Install::Installed
		} else {
			Install::NothingCacheable
		}
	}

	fn install_partition(&self, scan: &RangeScan, span: &Interval, rows: &[(EncodedKey, EncodedPodRow)]) -> bool {
		let Some(partition) = PartitionId::of(scan.operator, &span.start) else {
			return false;
		};
		if !partition.caches_ranges() {
			return false;
		}
		let (_, bound) = partition.span();
		if span.end > bound {
			return false;
		}

		let index = self.shard_index(&partition);
		let slot = partition.keyspace.0 as usize;
		let lands = rows.iter().any(|(key, _)| span.contains(key));
		let (fresh, inserted) = {
			let mut inserted = Vec::new();
			let mut shard = self.shard(index).lock();
			if !lands && !shard.partitions.contains_key(&partition) {
				drop(shard);
				return self.claim_only(scan, span, index, slot);
			}
			let tick = shard.next_tick;
			let Shard {
				partitions,
				budget,
				metrics,
				keyspace_metrics,
				..
			} = &mut *shard;

			let fresh = !partitions.contains_key(&partition);
			let resident = partitions.entry(partition).or_insert_with(|| Partition {
				entries: BTreeMap::new(),
				pinned: PinnedCount::new(),
				bytes: PARTITION_OVERHEAD,
				tick,
				installs: 0,
				covered: false,
			});

			let mut added = 0;
			for (key, row) in rows {
				if !span.contains(key) || resident.entries.contains_key(key) {
					continue;
				}
				let entry = Entry::row(row.clone());
				added += entry_footprint(key, &entry);
				resident.pinned.insert(&entry);
				resident.entries.insert(key.clone(), entry);
				inserted.push(key.clone());
			}

			let charge = added + if fresh {
				PARTITION_OVERHEAD
			} else {
				0
			};
			if !budget.try_charge(ByteSize::from_bytes(charge as u64)) {
				for key in &inserted {
					if let Some(entry) = resident.entries.remove(key) {
						resident.pinned.remove(&entry);
					}
				}
				if fresh {
					partitions.remove(&partition);
				}
				metrics.installs_refused += 1;
				keyspace_metrics[slot].installs_refused += 1;
				return false;
			}

			resident.bytes += added;
			resident.installs += 1;
			resident.tick = tick;
			resident.covered = true;
			shard.next_tick = tick + 1;
			(fresh, inserted)
		};

		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, partition);
		}

		{
			let mut coverage = self.coverage().write();
			if self.retractions() != scan.retractions {
				drop(coverage);
				self.roll_back_install(index, partition, fresh, &inserted);
				let mut shard = self.shard(index).lock();
				shard.metrics.installs_raced += 1;
				shard.keyspace_metrics[slot].installs_raced += 1;
				return false;
			}
			coverage.operators
				.entry(scan.operator)
				.or_default()
				.extend(span.start.clone(), span.end.clone());
		}

		let mut shard = self.shard(index).lock();
		shard.metrics.installs += 1;
		shard.keyspace_metrics[slot].installs += 1;
		true
	}

	fn claim_only(&self, scan: &RangeScan, span: &Interval, index: usize, slot: usize) -> bool {
		{
			let mut coverage = self.coverage().write();
			if self.retractions() != scan.retractions {
				drop(coverage);
				let mut shard = self.shard(index).lock();
				shard.metrics.installs_raced += 1;
				shard.keyspace_metrics[slot].installs_raced += 1;
				return false;
			}
			coverage.operators
				.entry(scan.operator)
				.or_default()
				.extend(span.start.clone(), span.end.clone());
		}

		let mut shard = self.shard(index).lock();
		shard.metrics.installs += 1;
		shard.keyspace_metrics[slot].installs += 1;
		true
	}

	fn roll_back_install(&self, index: usize, partition: PartitionId, fresh: bool, inserted: &[EncodedKey]) {
		let mut shard = self.shard(index).lock();
		let Shard {
			partitions,
			budget,
			..
		} = &mut *shard;
		let Some(resident) = partitions.get_mut(&partition) else {
			return;
		};

		let mut released = 0;
		for key in inserted {
			let Some(entry) = resident.entries.remove(key) else {
				continue;
			};
			if !matches!(entry.residency, Residency::Row(_)) {
				resident.entries.insert(key.clone(), entry);
				continue;
			}
			released += entry_footprint(key, &entry);
			resident.pinned.remove(&entry);
		}
		resident.bytes -= released;

		if fresh && resident.entries.is_empty() {
			released += resident.bytes;
			partitions.remove(&partition);
		}
		budget.release(ByteSize::from_bytes(released as u64));
	}
}

#[derive(Clone, Copy)]
enum Tally {
	Hit,
	Miss,
	Untallied,
}

fn pad_to_prefix(key: &EncodedKey) -> EncodedKey {
	let mut padded = key.as_slice().to_vec();
	padded.resize(PartitionId::PREFIX_LEN, 0);
	EncodedKey::new(padded)
}

fn partition_at(operator: OperatorId, key: &EncodedKey) -> Option<PartitionId> {
	match PartitionId::of(operator, key) {
		Some(partition) => Some(partition),
		None => PartitionId::of(operator, &pad_to_prefix(key)),
	}
}

fn exempt_gap(operator: OperatorId, gap: &Interval) -> bool {
	let Some(partition) = PartitionId::of(operator, &gap.start) else {
		return false;
	};
	if partition.caches_ranges() {
		return false;
	}
	let (_, end) = partition.span();
	gap.end <= end
}

fn coalesce_gaps(pieces: Vec<(Segment, Option<PartitionId>)>) -> Vec<(Segment, Option<PartitionId>)> {
	let mut out: Vec<(Segment, Option<PartitionId>)> = Vec::with_capacity(pieces.len());
	for (segment, partition) in pieces {
		let Segment::Gap {
			interval,
			exempt,
		} = &segment
		else {
			out.push((segment, partition));
			continue;
		};
		let merged = match (out.last_mut(), partition) {
			(
				Some((
					Segment::Gap {
						interval: prev,
						exempt: prev_exempt,
					},
					Some(_),
				)),
				Some(_),
			) if *prev_exempt == *exempt && prev.end == Edge::Key(interval.start.clone()) => {
				prev.end = interval.end.clone();
				true
			}
			_ => false,
		};
		if !merged {
			out.push((segment, partition));
		}
	}
	out
}

fn split_at_partitions(operator: OperatorId, segment: &Segment, out: &mut Vec<(Segment, Option<PartitionId>)>) {
	let (whole, ram) = match segment {
		Segment::Ram(interval) => (interval, true),
		Segment::Gap {
			interval,
			..
		} => (interval, false),
	};

	let mut start = whole.start.clone();
	let mut head = true;
	loop {
		if !whole.end.covers(&start) {
			return;
		}
		let Some(partition) = PartitionId::of(operator, &start) else {
			let bound = if head {
				Edge::Key(pad_to_prefix(&start))
			} else {
				whole.end.clone()
			};
			let end = bound.min(whole.end.clone());
			out.push((
				Segment::Gap {
					interval: Interval::new(start, end.clone()),
					exempt: false,
				},
				None,
			));
			head = false;
			match end {
				_ if end == whole.end => return,
				Edge::Key(key) => {
					start = key;
					continue;
				}
				Edge::Top => return,
			}
		};
		head = false;

		let (_, bound) = partition.span();
		let end = bound.min(whole.end.clone());
		let piece = Interval::new(start, end.clone());
		out.push((
			if ram {
				Segment::Ram(piece)
			} else {
				Segment::Gap {
					interval: piece,
					exempt: !partition.caches_ranges(),
				}
			},
			Some(partition),
		));

		if end == whole.end {
			return;
		}
		match end {
			Edge::Key(key) => start = key,
			Edge::Top => return,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::pod::EncodedPodRow,
	};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
	};
	use reifydb_store::coverage::{Edge, Interval, RangeCursor, Segment, ServedChunk};
	use reifydb_value::byte_size::ByteSize;

	use crate::tier::range::{Install, OperatorRangeConfig, OperatorRangeTier, PartitionId, RangeScan};

	const OP: OperatorId = OperatorId(1);
	const GROUP: GroupId = GroupId(10);
	const CACHED: Keyspace = Keyspace::ACCUMULATOR;
	const UNCACHED: Keyspace = Keyspace::ENGINE_META;

	fn tier(limit: u64, gap_guard: usize) -> OperatorRangeTier {
		OperatorRangeTier::new(OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_bytes(limit)),
			shards: 1,
			gap_guard,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn roomy() -> OperatorRangeTier {
		tier(ByteSize::from_mib(1).as_bytes(), 4)
	}

	fn key(keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn partition(keyspace: Keyspace) -> PartitionId {
		PartitionId {
			operator: OP,
			group: GROUP,
			keyspace,
		}
	}

	fn whole(keyspace: Keyspace) -> Interval {
		let (start, end) = partition(keyspace).span();
		Interval::new(start, end)
	}

	/// A range from the start of `top` to the end of `bottom`; keyspaces encode inverted, so `top`
	/// must be the numerically larger of the two to give an ascending key range.
	fn across(top: Keyspace, bottom: Keyspace) -> EncodedKeyRange {
		EncodedKeyRange::new(Bound::Included(key(top, b"")), keyspace_inner_range(GROUP, bottom).end)
	}

	fn claim(
		tier: &OperatorRangeTier,
		range: &EncodedKeyRange,
		span: &Interval,
		rows: &[(EncodedKey, EncodedPodRow)],
	) -> Install {
		let scan = tier.plan_scan(OP, range).expect("the fixture range must be plannable");
		tier.install(&scan, span, rows)
	}

	fn spanning(from: &EncodedKey, to: &EncodedKey) -> Interval {
		Interval::new(from.clone(), Edge::Key(to.clone()))
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

	fn intervals(tier: &OperatorRangeTier) -> Vec<Interval> {
		tier.coverage().read().operators.get(&OP).map(|set| set.iter().collect()).unwrap_or_default()
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
		) == Install::Installed);
		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"f"), &at(b"h")),
			&[(at(b"f"), row("f")), (at(b"g"), row("g"))]
		) == Install::Installed);

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		let keyspace = whole(CACHED);

		assert_eq!(
			scan.segments(),
			[
				Segment::Gap {
					interval: Interval::new(keyspace.start.clone(), Edge::Key(at(b"c"))),
					exempt: false,
				},
				Segment::Ram(spanning(&at(b"c"), &at(b"e"))),
				Segment::Gap {
					interval: spanning(&at(b"e"), &at(b"f")),
					exempt: false,
				},
				Segment::Ram(spanning(&at(b"f"), &at(b"h"))),
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
	fn two_overlapping_installs_compose_instead_of_clobbering_each_other() {
		// A re-read key must not overwrite the resident row, nor drop the keys only the second read saw.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);

		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"a"), &at(b"c")),
			&[(at(b"a"), row("a1")), (at(b"b"), row("b1"))]
		) == Install::Installed);
		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"b"), &at(b"e")),
			&[(at(b"b"), row("b2")), (at(b"c"), row("c1")), (at(b"d"), row("d1"))]
		) == Install::Installed);

		assert_eq!(intervals(&tier), [spanning(&at(b"a"), &at(b"e"))], "two touching claims must coalesce");

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		assert_eq!(
			drain(&tier, &scan, &spanning(&at(b"a"), &at(b"e")), 64),
			["a1", "b1", "c1", "d1"],
			"the resident row must survive the overlap, and the new keys must join it"
		);
	}

	#[test]
	fn a_plan_with_more_non_exempt_gaps_than_the_guard_degrades_to_one_full_scan() {
		// A plan of many small persistent reads must be abandoned, or the caller pays a trip per hole.
		let tier = tier(ByteSize::from_mib(1).as_bytes(), 1);
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);

		assert!(claim(&tier, &range, &spanning(&at(b"b"), &at(b"c")), &[(at(b"b"), row("b"))])
			== Install::Installed);
		assert!(claim(&tier, &range, &spanning(&at(b"d"), &at(b"e")), &[(at(b"d"), row("d"))])
			== Install::Installed);

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
		let tier = tier(ByteSize::from_mib(1).as_bytes(), 1);
		let range = across(UNCACHED, Keyspace::EXPIRY);

		for raw in (Keyspace::EXPIRY.0 + 1)..UNCACHED.0 {
			let keyspace = Keyspace(raw);
			assert!(
				keyspace.cache_policy().caches_ranges(),
				"the fixture middle must be cacheable, or it proves nothing"
			);
			assert!(
				claim(&tier, &range, &whole(keyspace), &[]) == Install::Installed,
				"an empty proven span is still a claim"
			);
		}

		let scan = tier.plan_scan(OP, &range).expect("a cross-keyspace range must be plannable");
		assert_eq!(scan.gaps(), 0, "both remaining gaps lie in keyspaces that are never cached");
		assert!(
			!scan.degraded(),
			"two exempt gaps against a guard of one must not degrade, or every cross-keyspace read \
             collapses to a full scan forever"
		);
		assert!(
			scan.segments().iter().any(|segment| matches!(
				segment,
				Segment::Gap {
					exempt: true,
					..
				}
			)),
			"the fixture must actually produce an exempt gap"
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
		) == Install::Installed);
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
	fn an_install_refused_for_the_budget_leaves_the_tier_exactly_as_it_found_it() {
		// A refusal that keeps its rows lets a later read answer from a row no claim ever proved.
		let tier = tier(512, 4);
		let range = keyspace_inner_range(GROUP, CACHED);
		let page: Vec<(EncodedKey, EncodedPodRow)> =
			(0..64u8).map(|index| (key(CACHED, &[index]), row("a fairly long row body"))).collect();

		assert!(
			claim(&tier, &range, &whole(CACHED), &page) == Install::Refused,
			"a span past the shard limit must be refused"
		);

		assert_eq!(tier.partitions(), 0, "a refused install must leave no partition behind");
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0, "and no claim over the span it failed to install");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a refused install must not be charged a byte");
		assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
		assert_eq!(tier.lookup(OP, &key(CACHED, &[0u8])), None);
		assert_eq!(tier.metrics().installs_refused, 1);
		assert_eq!(tier.metrics().installs, 0);
	}

	#[test]
	fn an_install_into_a_keyspace_that_is_never_cached_leaves_the_tier_exactly_as_it_found_it() {
		// Taking these rows would admit a keyspace the tier is configured never to hold.
		let tier = roomy();
		let range = across(UNCACHED, Keyspace(UNCACHED.0 - 1));
		let at = key(UNCACHED, b"a");

		assert!(
			claim(&tier, &range, &whole(UNCACHED), &[(at.clone(), row("v"))]) == Install::NothingCacheable,
			"a span holding no cacheable partition must report nothing to cache, never refusal, or the \
             caller stops installing for the rest of the scan"
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
		assert_eq!(tier.metrics().installs, 0);
	}

	#[test]
	fn an_install_that_races_a_retraction_leaves_the_tier_exactly_as_it_found_it() {
		// Extending coverage after a retraction reinstates a claim over a row the writer removed.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = key(CACHED, b"a");
		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");

		tier.record_retraction();

		assert!(tier.install(&scan, &whole(CACHED), &[(at.clone(), row("v"))]) == Install::Refused);

		assert_eq!(tier.partitions(), 0, "a refused install must roll back the partition it created");
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0, "and leave no claim behind over the span it failed to install");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
		assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
		assert_eq!(tier.lookup(OP, &at), None, "the rolled back row must fall through, not answer");
		assert_eq!(tier.metrics().installs_raced, 1);
		assert_eq!(tier.metrics().installs, 0);
	}

	#[test]
	fn a_cross_keyspace_span_is_split_so_every_segment_lies_in_one_partition() {
		// A coalesced claim must split per partition, or only the segment start's partition answers.
		let tier = roomy();
		let top = Keyspace::SESSION;
		let middle = Keyspace(top.0 - 1);
		let bottom = Keyspace(top.0 - 2);
		let range = across(top, bottom);

		for keyspace in [top, middle, bottom] {
			assert!(keyspace.cache_policy().caches_ranges());
			assert!(claim(
				&tier,
				&range,
				&whole(keyspace),
				&[(key(keyspace, b"k"), row(&keyspace.name()))]
			) == Install::Installed);
		}

		assert_eq!(intervals(&tier).len(), 1, "the three claims must coalesce, or the split is not under test");

		let scan = tier.plan_scan(OP, &range).expect("a cross-keyspace range must be plannable");
		assert_eq!(
			scan.segments(),
			[Segment::Ram(whole(top)), Segment::Ram(whole(middle)), Segment::Ram(whole(bottom))],
			"one coalesced claim must split into one segment per partition, in ascending key order"
		);

		let served: Vec<String> = scan
			.segments()
			.iter()
			.flat_map(|segment| match segment {
				Segment::Ram(interval) => drain(&tier, &scan, interval, 64),
				Segment::Gap {
					..
				} => Vec::new(),
			})
			.collect();
		assert_eq!(
			served,
			[top.name(), middle.name(), bottom.name()],
			"every partition must answer its own slice, and none may be skipped"
		);
	}

	#[test]
	fn an_install_whose_span_crosses_a_partition_boundary_lands_rows_in_both_keyspaces() {
		// A coalesced gap hands install one span covering several keyspaces, so install must re-split it per partition; refusing the whole span instead would leave every cross-keyspace read permanently uncached.
		let tier = roomy();
		let top = Keyspace::BUFFER;
		let bottom = Keyspace::ACCUMULATOR;
		let head = key(top, b"a");
		let tail = key(bottom, b"m");
		let span = Interval::new(head.clone(), whole(bottom).end);
		let rows = [(head.clone(), row("top")), (tail.clone(), row("bottom"))];

		let scan = tier.plan_scan(OP, &across(top, bottom)).expect("a two-keyspace range must be plannable");
		assert!(
			tier.install(&scan, &span, &rows) == Install::Installed,
			"an install spanning two cached partitions must be accepted"
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
			"the partition past the boundary must be installed too, not discarded with the rest of the span"
		);
	}

	#[test]
	fn a_plan_merges_contiguous_gaps_into_one_read_but_never_across_an_exempt_boundary() {
		// One read per uncovered run is the point: splitting every gap at a keyspace boundary issued a separate store read per keyspace byte the scan crossed. Folding an exempt keyspace into a cached run would also hide it from the gap guard.
		let tier = roomy();
		let top = Keyspace::BUFFER;
		let bottom = Keyspace::ACCUMULATOR;

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
			.plan_scan(OP, &across(Keyspace::DISTINCT_ENTRY, Keyspace::ROLLING_META))
			.expect("a three-keyspace range must be plannable");
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
	fn an_install_that_proves_an_empty_span_claims_it_without_paying_for_a_partition() {
		// A span the persistent tier answered with nothing is worth claiming, but the claim lives in the
		// coverage index and the partition would hold no row. Charging one anyway lets a scan that crosses
		// many empty keyspaces spend the whole budget on structures holding nothing, which evicts the rows
		// the tier exists to serve.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);

		assert!(claim(&tier, &range, &whole(CACHED), &[]) == Install::Installed);

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

		assert!(claim(&tier, &range, &whole(CACHED), &[]) == Install::Installed);
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
