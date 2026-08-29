// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound::{Excluded, Included, Unbounded};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	key::typed::{ExclusiveUpperEnd, Key},
	util::sorted::SortedVecMap,
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	coverage::{
		cursor::{RangeCursor, ServedChunk},
		entry::{Entry, PinnedCount},
		interval::{CoverageSet, Interval},
		plan::{Segment, plan},
	},
	tier::range::{
		Materialize, Partition, RangeDomain, RangeRows, RangeScan, RangeTier, Shard, entry_footprint,
		head::advance_to_head, partition_overhead,
	},
};

enum PartitionAction {
	Claim(usize),
	Materialize,
}

impl<D: RangeDomain> RangeScan<D> {
	pub fn segments(&self) -> &[Segment] {
		&self.segments
	}

	pub fn gaps(&self) -> usize {
		self.gaps
	}

	pub fn degraded(&self) -> bool {
		self.degraded
	}

	pub fn dimension(&self) -> D::Dimension {
		self.dimension
	}

	pub fn advanced(&self) -> bool {
		self.advanced
	}
}

impl<D: RangeDomain> RangeTier<D> {
	pub fn plan_scan(&self, dimension: D::Dimension, range: &EncodedKeyRange) -> Option<RangeScan<D>> {
		let lo = match range.start.as_ref() {
			Included(key) => key.clone(),
			Excluded(key) => key.successor()?,
			Unbounded => return None,
		};
		let hi = match range.end.as_ref() {
			Included(key) => ExclusiveUpperEnd::just_past(key),
			Excluded(key) => ExclusiveUpperEnd::Key(key.clone()),
			Unbounded => ExclusiveUpperEnd::Top,
		};

		let (lo, head, planned, held, retractions, advanced) = {
			let coverage = self.coverage().read();
			let anchor = lo.clone();
			let lo = advance_to_head::<D>(&coverage, dimension, lo, &hi);
			let advanced = lo != anchor;
			let head = partition_at::<D>(dimension, &lo)?;
			let (_, head_end) = D::span(&head);
			if hi <= head_end && !D::caches_ranges(&head) {
				return None;
			}
			let vacant = CoverageSet::new();
			let claims = coverage.set(dimension).unwrap_or(&vacant);
			let planned = plan(claims, lo.clone(), hi.clone(), self.gap_guard(), |gap| {
				exempt_gap::<D>(dimension, gap)
			});
			let held = claims.contains(&lo);
			(lo, head, planned, held, self.retractions(), advanced)
		};

		let mut pieces = Vec::with_capacity(planned.segments.len().max(1));
		if planned.segments.is_empty() {
			let interval = Interval::new(lo, hi);
			pieces.push((
				if held {
					Segment::Resident(interval)
				} else {
					Segment::Gap {
						interval,
						exempt: !D::caches_ranges(&head),
					}
				},
				Some(head),
			));
		}
		for segment in &planned.segments {
			split_at_partitions::<D>(dimension, segment, &mut pieces);
		}
		let pieces = coalesce_gaps::<D>(pieces);
		if pieces.is_empty() {
			return Some(RangeScan {
				dimension,
				advanced,
				segments: Vec::new(),
				gaps: planned.gaps - planned.exempted,
				degraded: planned.degraded,
				retractions,
			});
		}

		let head_shard = self.shard_index(&head);
		let mut orphaned = 0;
		let mut work = Vec::with_capacity(pieces.len());
		for (at, (segment, partition)) in pieces.iter().enumerate() {
			let tally = match segment {
				Segment::Resident(_) => Tally::Hit,
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
				Some(partition) => {
					work.push((self.shard_index(partition), Some(*partition), tally, at))
				}
				None => {
					if matches!(tally, Tally::Miss) {
						orphaned += 1;
					}
				}
			}
		}
		work.sort_by_key(|(index, _, _, _)| *index);
		if orphaned > 0 {
			work.push((head_shard, None, Tally::Untallied, pieces.len()));
		}

		let mut barren: Option<Vec<bool>> = None;
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
				let (_, partition, tally, piece) = work[at];
				at += 1;
				let Some(partition) = partition else {
					continue;
				};
				let slot = D::slot(&partition);
				match tally {
					Tally::Hit => {
						shard.metrics.hits += 1;
						shard.slot_metrics[slot].hits += 1;
					}
					Tally::Miss => {
						shard.metrics.misses += 1;
						shard.slot_metrics[slot].misses += 1;
					}
					Tally::Untallied => {
						shard.metrics.exempt += 1;
						shard.slot_metrics[slot].exempt += 1;
					}
				}
				if D::caches_ranges(&partition) {
					match shard.partitions.get_mut(&partition) {
						Some(resident) => resident.covered = true,
						None => {
							if matches!(tally, Tally::Hit) {
								barren.get_or_insert_with(|| {
									vec![false; pieces.len()]
								})[piece] = true;
							}
						}
					}
				}
			}
		}

		let pieces = match barren {
			Some(barren) if self.retractions_unchanged(retractions) => pieces
				.into_iter()
				.zip(barren)
				.filter(|(_, drop)| !drop)
				.map(|(piece, _)| piece)
				.collect(),
			_ => pieces,
		};

		Some(RangeScan {
			dimension,
			advanced,
			segments: pieces.into_iter().map(|(segment, _)| segment).collect(),
			gaps: planned.gaps - planned.exempted,
			degraded: planned.degraded,
			retractions,
		})
	}

	pub fn serve(
		&self,
		scan: &RangeScan<D>,
		segment: &Interval,
		cursor: &mut RangeCursor,
		limit: usize,
	) -> ServedChunk<RangeRows<D>> {
		let start = match cursor.last_key() {
			Some(last) if last.as_slice() >= segment.start.as_slice() => last.successor(),
			_ => Some(segment.start.clone()),
		};
		let Some(start) = start.filter(|start| segment.end.covers(start)) else {
			cursor.finish();
			return ServedChunk::Served(Vec::new());
		};

		let Some(partition) = D::partition(scan.dimension, &segment.start) else {
			return ServedChunk::Gap;
		};
		if !D::caches_ranges(&partition) {
			return ServedChunk::Gap;
		}

		let observed = {
			let coverage = self.coverage().read();
			let Some(claims) = coverage.set(scan.dimension) else {
				return ServedChunk::Gap;
			};
			match claims.covering(&start) {
				Some(claim) if claim.end >= segment.end => self.retractions(),
				_ => return ServedChunk::Gap,
			}
		};

		#[cfg(test)]
		self.fire_serve_interlock();

		let limit = limit.max(1);
		let mut rows: RangeRows<D> = Vec::new();
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
						ExclusiveUpperEnd::Key(key) => Excluded(key.clone()),
						ExclusiveUpperEnd::Top => Unbounded,
					};
					let span = (Included(start), upper);
					for (key, entry) in resident.entries.range(span) {
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

		if !self.retractions_unchanged(observed) {
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

	pub fn materialize(&self, scan: &RangeScan<D>, span: &Interval, rows: &[(EncodedKey, D::Row)]) -> Materialize {
		let mut start = span.start.clone();
		let mut head = true;
		let mut materialized = false;
		let mut claim: Option<Interval> = None;
		let mut claimed: Vec<usize> = Vec::new();
		if !span.is_empty() {
			loop {
				let Some(partition) = D::partition(scan.dimension, &start) else {
					let Some(anchor) = rows
						.first()
						.and_then(|(key, _)| D::partition(scan.dimension, key))
						.map(|at| D::span(&at).0)
					else {
						break;
					};
					if !head || anchor.as_slice() <= start.as_slice() || !span.end.covers(&anchor) {
						break;
					}
					head = false;
					start = anchor;
					continue;
				};
				head = false;
				let (_, bound) = D::span(&partition);
				let end = bound.min(span.end.clone());
				let piece = Interval::new(start, end.clone());
				if !D::caches_ranges(&partition) || piece.is_empty() {
					if !self.flush_claims(scan, &mut claim, &mut claimed) {
						return Materialize::Refused;
					}
				} else {
					match self.classify(&piece, partition, rows) {
						PartitionAction::Claim(index) => {
							match &mut claim {
								Some(run) => run.end = piece.end.clone(),
								None => claim = Some(piece.clone()),
							}
							claimed.push(index);
							materialized = true;
						}
						PartitionAction::Materialize => {
							if !self.flush_claims(scan, &mut claim, &mut claimed) {
								return Materialize::Refused;
							}
							if !self.materialize_partition(scan, &piece, rows) {
								return Materialize::Refused;
							}
							materialized = true;
						}
					}
				}
				if end == span.end {
					break;
				}
				match end {
					ExclusiveUpperEnd::Key(key) => start = key,
					ExclusiveUpperEnd::Top => break,
				}
			}
		}
		if !self.flush_claims(scan, &mut claim, &mut claimed) {
			return Materialize::Refused;
		}
		if materialized {
			Materialize::Materialized
		} else {
			Materialize::NothingCacheable
		}
	}

	fn classify(
		&self,
		piece: &Interval,
		partition: D::Partition,
		rows: &[(EncodedKey, D::Row)],
	) -> PartitionAction {
		if rows.iter().any(|(key, _)| piece.contains(key)) {
			return PartitionAction::Materialize;
		}
		let index = self.shard_index(&partition);
		let mut shard = self.shard(index).lock();
		if shard.partitions.contains_key(&partition) {
			return PartitionAction::Materialize;
		}
		shard.metrics.materializes += 1;
		PartitionAction::Claim(index)
	}

	fn flush_claims(&self, scan: &RangeScan<D>, claim: &mut Option<Interval>, claimed: &mut Vec<usize>) -> bool {
		let Some(span) = claim.take() else {
			return true;
		};
		let mut coverage = self.coverage().write();
		if !self.retractions_unchanged(scan.retractions) {
			drop(coverage);
			self.undo_claims(claimed);
			return false;
		}
		coverage.extend(scan.dimension, span.start, span.end);
		drop(coverage);
		claimed.clear();
		true
	}

	fn undo_claims(&self, claimed: &mut Vec<usize>) {
		claimed.sort_unstable();
		let mut cursor = 0;
		while cursor < claimed.len() {
			let index = claimed[cursor];
			let mut shard = self.shard(index).lock();
			while cursor < claimed.len() && claimed[cursor] == index {
				shard.metrics.materializes -= 1;
				shard.metrics.materializes_raced += 1;
				cursor += 1;
			}
		}
		claimed.clear();
	}

	fn materialize_partition(&self, scan: &RangeScan<D>, span: &Interval, rows: &[(EncodedKey, D::Row)]) -> bool {
		let Some(partition) = D::partition(scan.dimension, &span.start) else {
			return false;
		};
		if !D::caches_ranges(&partition) {
			return false;
		}
		let (_, bound) = D::span(&partition);
		if span.end > bound {
			return false;
		}

		let index = self.shard_index(&partition);
		let slot = D::slot(&partition);
		let lands = rows.iter().any(|(key, _)| span.contains(key));
		let (fresh, inserted, writes) = {
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
				slot_metrics,
				..
			} = &mut *shard;

			let fresh = !partitions.contains_key(&partition);
			let resident = partitions.entry(partition).or_insert_with(|| Partition {
				entries: SortedVecMap::new(),
				pinned: PinnedCount::new(),
				bytes: partition_overhead::<D>(),
				tick,
				created: tick,
				materializes: 0,
				written_at: 0,
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
				partition_overhead::<D>()
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
				metrics.materializes_refused += 1;
				slot_metrics[slot].materializes_refused += 1;
				return false;
			}

			resident.bytes += added;
			resident.materializes += 1;
			resident.tick = tick;
			resident.covered = true;
			shard.next_tick = tick + 1;
			(fresh, inserted, shard.writes)
		};

		#[cfg(test)]
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, partition);
		}

		{
			let mut coverage = self.coverage().write();
			if !self.retractions_unchanged(scan.retractions) {
				drop(coverage);
				self.roll_back_materialize(index, partition, fresh, &inserted, writes);
				let mut shard = self.shard(index).lock();
				shard.metrics.materializes_raced += 1;
				shard.slot_metrics[slot].materializes_raced += 1;
				return false;
			}
			coverage.extend(scan.dimension, span.start.clone(), span.end.clone());
		}

		let mut shard = self.shard(index).lock();
		shard.metrics.materializes += 1;
		shard.slot_metrics[slot].materializes += 1;
		true
	}

	fn claim_only(&self, scan: &RangeScan<D>, span: &Interval, index: usize, slot: usize) -> bool {
		{
			let mut coverage = self.coverage().write();
			if !self.retractions_unchanged(scan.retractions) {
				drop(coverage);
				let mut shard = self.shard(index).lock();
				shard.metrics.materializes_raced += 1;
				shard.slot_metrics[slot].materializes_raced += 1;
				return false;
			}
			coverage.extend(scan.dimension, span.start.clone(), span.end.clone());
		}

		let mut shard = self.shard(index).lock();
		shard.metrics.materializes += 1;
		shard.slot_metrics[slot].materializes += 1;
		true
	}

	fn roll_back_materialize(
		&self,
		index: usize,
		partition: D::Partition,
		fresh: bool,
		inserted: &[EncodedKey],
		writes: u64,
	) {
		let dimension = D::dimension(&partition);
		for key in inserted {
			self.withdraw(dimension, key);
		}
		self.drop_placed(index, partition, fresh, inserted, writes);
		for key in inserted {
			self.withdraw(dimension, key);
		}
	}

	fn drop_placed(
		&self,
		index: usize,
		partition: D::Partition,
		fresh: bool,
		inserted: &[EncodedKey],
		writes: u64,
	) {
		let mut shard = self.shard(index).lock();
		if shard.writes != writes {
			return;
		}
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
			if !matches!(entry, Entry::Row(_)) {
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

fn pad_to_prefix<D: RangeDomain>(key: &EncodedKey) -> EncodedKey {
	let mut padded = key.as_slice().to_vec();
	padded.resize(D::PREFIX_LEN, 0);
	EncodedKey::new(padded)
}

fn partition_at<D: RangeDomain>(dimension: D::Dimension, key: &EncodedKey) -> Option<D::Partition> {
	match D::partition(dimension, key) {
		Some(partition) => Some(partition),
		None => D::partition(dimension, &pad_to_prefix::<D>(key)),
	}
}

fn unaddressable_gap<D: RangeDomain>(gap: &Interval) -> bool {
	gap.start.as_slice().len() < D::PREFIX_LEN && gap.end <= ExclusiveUpperEnd::Key(pad_to_prefix::<D>(&gap.start))
}

fn exempt_gap<D: RangeDomain>(dimension: D::Dimension, gap: &Interval) -> bool {
	if unaddressable_gap::<D>(gap) {
		return true;
	}
	let Some(partition) = D::partition(dimension, &gap.start) else {
		return false;
	};
	if D::caches_ranges(&partition) {
		return false;
	}
	let (_, end) = D::span(&partition);
	gap.end <= end
}

fn coalesce_gaps<D: RangeDomain>(pieces: Vec<(Segment, Option<D::Partition>)>) -> Vec<(Segment, Option<D::Partition>)> {
	let mut out: Vec<(Segment, Option<D::Partition>)> = Vec::with_capacity(pieces.len());
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
			) if *prev_exempt == *exempt && prev.end == ExclusiveUpperEnd::Key(interval.start.clone()) => {
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

fn split_at_partitions<D: RangeDomain>(
	dimension: D::Dimension,
	segment: &Segment,
	out: &mut Vec<(Segment, Option<D::Partition>)>,
) {
	let (whole, ram) = match segment {
		Segment::Resident(interval) => (interval, true),
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
		let Some(partition) = D::partition(dimension, &start) else {
			let bound = if head {
				ExclusiveUpperEnd::Key(pad_to_prefix::<D>(&start))
			} else {
				whole.end.clone()
			};
			let end = bound.min(whole.end.clone());
			let interval = Interval::new(start, end.clone());
			if !unaddressable_gap::<D>(&interval) {
				out.push((
					Segment::Gap {
						interval,
						exempt: false,
					},
					None,
				));
			}
			head = false;
			match end {
				_ if end == whole.end => return,
				ExclusiveUpperEnd::Key(key) => {
					start = key;
					continue;
				}
				ExclusiveUpperEnd::Top => return,
			}
		};
		head = false;

		let bound = if ram {
			D::span(&partition).1
		} else {
			D::cache_tiers_run_end(&partition)
		};
		let end = bound.min(whole.end.clone());
		let piece = Interval::new(start, end.clone());
		out.push((
			if ram {
				Segment::Resident(piece)
			} else {
				Segment::Gap {
					interval: piece,
					exempt: !D::caches_ranges(&partition),
				}
			},
			Some(partition),
		));

		if end == whole.end {
			return;
		}
		match end {
			ExclusiveUpperEnd::Key(key) => start = key,
			ExclusiveUpperEnd::Top => return,
		}
	}
}

#[cfg(test)]
mod tests {
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
		key::{
			operator::state::{GroupId, KeyspaceId, OperatorStateKey, keyspace_inner_range},
			typed::ExclusiveUpperEnd,
		},
	};
	use reifydb_value::byte_size::ByteSize;

	use super::split_at_partitions;
	use crate::{
		coverage::{
			cursor::{RangeCursor, ServedChunk},
			interval::Interval,
			plan::Segment,
		},
		tier::range::{
			Materialize, RangeConfig, RangeScan, RangeTier,
			domain::{TestDomain as D, TestPartition},
		},
	};

	const OP: OperatorId = OperatorId(1);
	const GROUP: GroupId = GroupId(10);
	const CACHED: KeyspaceId = KeyspaceId::ACCUMULATOR;
	const UNCACHED: KeyspaceId = KeyspaceId::CUSTOM_NOT_CACHED;

	fn tier(limit: u64, gap_guard: usize) -> RangeTier<D> {
		RangeTier::<D>::new(RangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(limit)),
			shards: 1,
			gap_guard,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn roomy() -> RangeTier<D> {
		tier(ByteSize::from_mib(1).as_bytes(), 4)
	}

	fn key(keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn partition(keyspace: KeyspaceId) -> TestPartition {
		TestPartition {
			dimension: OP,
			group: GROUP,
			slot: keyspace,
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
		tier: &RangeTier<D>,
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

	fn drain(tier: &RangeTier<D>, scan: &RangeScan<D>, segment: &Interval, limit: usize) -> Vec<String> {
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

	fn intervals(tier: &RangeTier<D>) -> Vec<Interval> {
		tier.coverage().read().set(OP).map(|set| set.iter().collect()).unwrap_or_default()
	}

	#[test]
	fn two_overlapping_materializes_compose_instead_of_clobbering_each_other() {
		// A re-read key must not overwrite the resident row, nor drop the keys only the second read saw.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = |suffix: &[u8]| key(CACHED, suffix);

		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"a"), &at(b"c")),
			&[(at(b"a"), row("a1")), (at(b"b"), row("b1"))]
		) == Materialize::Materialized);
		assert!(claim(
			&tier,
			&range,
			&spanning(&at(b"b"), &at(b"e")),
			&[(at(b"b"), row("b2")), (at(b"c"), row("c1")), (at(b"d"), row("d1"))]
		) == Materialize::Materialized);

		assert_eq!(intervals(&tier), [spanning(&at(b"a"), &at(b"e"))], "two touching claims must coalesce");

		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		assert_eq!(
			drain(&tier, &scan, &spanning(&at(b"a"), &at(b"e")), 64),
			["a1", "b1", "c1", "d1"],
			"the resident row must survive the overlap, and the new keys must join it"
		);
	}

	#[test]
	fn a_materialize_refused_for_the_budget_leaves_the_tier_exactly_as_it_found_it() {
		// A refusal that keeps its rows lets a later read answer from a row no claim ever proved.
		let tier = tier(512, 4);
		let range = keyspace_inner_range(GROUP, CACHED);
		let page: Vec<(EncodedKey, EncodedPodRow)> =
			(0..64u8).map(|index| (key(CACHED, &[index]), row("a fairly long row body"))).collect();

		assert!(
			claim(&tier, &range, &whole(CACHED), &page) == Materialize::Refused,
			"a span past the shard limit must be refused"
		);

		assert_eq!(tier.partitions(), 0, "a refused materialize must leave no partition behind");
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0, "and no claim over the span it failed to materialize");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "a refused materialize must not be charged a byte");
		assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
		assert_eq!(tier.lookup(OP, &key(CACHED, &[0u8])), None);
		assert_eq!(tier.metrics().materializes_refused, 1);
		assert_eq!(tier.metrics().materializes, 0);
	}

	#[test]
	fn a_materialize_that_races_a_retraction_leaves_the_tier_exactly_as_it_found_it() {
		// Extending coverage after a retraction reinstates a claim over a row the writer removed.
		let tier = roomy();
		let range = keyspace_inner_range(GROUP, CACHED);
		let at = key(CACHED, b"a");
		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");

		tier.record_retraction();

		assert!(tier.materialize(&scan, &whole(CACHED), &[(at.clone(), row("v"))]) == Materialize::Refused);

		assert_eq!(tier.partitions(), 0, "a refused materialize must roll back the partition it created");
		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0, "and leave no claim behind over the span it failed to materialize");
		assert_eq!(tier.resident_bytes(), ByteSize::ZERO);
		assert_eq!(tier.resident_bytes(), tier.tallied_bytes());
		assert_eq!(tier.lookup(OP, &at), None, "the rolled back row must fall through, not answer");
		assert_eq!(tier.metrics().materializes_raced, 1);
		assert_eq!(tier.metrics().materializes, 0);
	}

	#[test]
	fn a_cross_keyspace_span_is_split_so_every_segment_lies_in_one_partition() {
		// A coalesced claim must split per partition, or only the segment start's partition answers.
		let tier = roomy();
		let top = KeyspaceId::SESSION;
		let middle = KeyspaceId(top.0 - 1);
		let bottom = KeyspaceId(top.0 - 2);
		let range = across(top, bottom);

		for keyspace in [top, middle, bottom] {
			assert!(keyspace.cache_tiers().caches_ranges());
			assert!(claim(
				&tier,
				&range,
				&whole(keyspace),
				&[(key(keyspace, b"k"), row(&keyspace.name()))]
			) == Materialize::Materialized);
		}

		assert_eq!(intervals(&tier).len(), 1, "the three claims must coalesce, or the split is not under test");

		let scan = tier.plan_scan(OP, &range).expect("a cross-keyspace range must be plannable");
		assert_eq!(
			scan.segments(),
			[
				Segment::Resident(whole(top)),
				Segment::Resident(whole(middle)),
				Segment::Resident(whole(bottom))
			],
			"one coalesced claim must split into one segment per partition, in ascending key order"
		);

		let served: Vec<String> = scan
			.segments()
			.iter()
			.flat_map(|segment| match segment {
				Segment::Resident(interval) => drain(&tier, &scan, interval, 64),
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
	fn a_wide_gap_splits_once_per_cache_tiers_run_while_ram_still_splits_once_per_partition() {
		// A gap piece per keyspace byte made a group-wide scan build ~97 pieces that coalesce_gaps then
		// merged back into ~3; the run is the unit that survives, so emitting bytes is pure waste. RAM must
		// keep splitting per partition, because serve resolves one partition from the segment start and a
		// merged RAM piece would silently answer only the first partition's rows.
		let top = KeyspaceId(UNCACHED.0 + 8);
		let below = KeyspaceId(UNCACHED.0 - 1);
		let bottom = KeyspaceId(UNCACHED.0 - 8);
		let span = Interval::new(whole(top).start, whole(bottom).end);

		let mut gap = Vec::new();
		split_at_partitions::<D>(
			OP,
			&Segment::Gap {
				interval: span.clone(),
				exempt: false,
			},
			&mut gap,
		);
		assert_eq!(
			gap,
			[
				(
					Segment::Gap {
						interval: Interval::new(
							whole(top).start,
							ExclusiveUpperEnd::Key(whole(UNCACHED).start)
						),
						exempt: false,
					},
					Some(partition(top))
				),
				(
					Segment::Gap {
						interval: whole(UNCACHED),
						exempt: true,
					},
					Some(partition(UNCACHED))
				),
				(
					Segment::Gap {
						interval: Interval::new(whole(below).start, whole(bottom).end),
						exempt: false,
					},
					Some(partition(below))
				),
			],
			"seventeen keyspaces holding one exempt byte must split into three cache tier runs, each carrying \
			 the first partition of its run so the miss tally stays per run"
		);

		let mut ram = Vec::new();
		split_at_partitions::<D>(OP, &Segment::Resident(span), &mut ram);
		assert_eq!(
			ram.len(),
			(bottom.0..=top.0).len(),
			"a RAM segment must still yield one piece per partition, or serve answers only the first"
		);
		assert!(
			ram.iter()
				.all(|(segment, partition)| matches!(segment, Segment::Resident(_))
					&& partition.is_some()),
			"every RAM piece must name the partition that serves it"
		);
	}

	#[test]
	fn a_claim_over_a_partition_holding_nothing_plans_no_segment_and_no_gap() {
		// A claim can outlive its partition, so most RAM pieces address a partition that was never
		// materialised; serving one took a coverage read lock and a shard mutex to return zero rows. Dropping
		// the piece is only safe if the span does not reappear as a gap: a gap sends the reader to the
		// persistent tier for a span the claim already proved empty, which is both a wasted read and, once
		// the claim is the only proof, a different answer.
		let tier = roomy();
		let top = KeyspaceId::BUFFER;
		let bottom = KeyspaceId::ACCUMULATOR;
		let range = across(top, bottom);

		assert!(claim(&tier, &range, &whole(top), &[]) == Materialize::Materialized);
		assert!(claim(&tier, &range, &whole(bottom), &[(key(bottom, b"k"), row("k"))])
			== Materialize::Materialized);
		assert_eq!(intervals(&tier).len(), 1, "the two claims must coalesce, or the drop is not under test");
		assert_eq!(tier.partitions(), 1, "only the keyspace holding a row may hold a partition");

		let before = tier.metrics().hits;
		let scan = tier.plan_scan(OP, &range).expect("a two-keyspace range must be plannable");

		assert_eq!(
			scan.segments(),
			[Segment::Resident(whole(bottom))],
			"the piece addressing the unmaterialised partition must be dropped, not served empty"
		);
		assert_eq!(
			scan.gaps(),
			0,
			"and the span it covered must not become a gap, or the reader pays a persistent read for a \
			 span the claim proved empty"
		);
		assert_eq!(
			tier.metrics().hits - before,
			2,
			"both pieces must still tally as hits, or dropping one rewrites the hit rate the tier reports"
		);
		assert_eq!(
			drain(&tier, &scan, &whole(bottom), 64),
			["k"],
			"the partition that does hold a row must still answer it"
		);
	}

	#[test]
	fn a_serve_refuses_a_plan_a_retraction_raced() {
		// The plan is read under the coverage lock and the rows under the shard lock, never both at once, so a
		// withdrawal can land between them and falsify the claim the plan was built from. Serving anyway hands
		// back rows the tier no longer holds and, worse, reports proven absence over the key it dropped.
		let raced = key(CACHED, b"b");
		let armed = Arc::new(AtomicBool::new(false));
		let tier = {
			let armed = armed.clone();
			let raced = raced.clone();
			RangeTier::<D>::with_serve_interlock(
				RangeConfig {
					shard_bytes: Some(ByteSize::from_mib(1)),
					shards: 1,
					gap_guard: 4,
				},
				Box::new(move |tier: &RangeTier<D>| {
					if armed.swap(false, Ordering::SeqCst) {
						tier.invalidate(OP, &raced);
					}
				}),
			)
			.expect("a tier with a byte budget must be constructed")
		};
		let range = keyspace_inner_range(GROUP, CACHED);
		assert!(
			claim(
				&tier,
				&range,
				&whole(CACHED),
				&[(key(CACHED, b"a"), row("a")), (raced.clone(), row("b"))]
			) == Materialize::Materialized,
			"the fixture must publish its claim, or the race under test never arose"
		);

		armed.store(true, Ordering::SeqCst);
		let scan = tier.plan_scan(OP, &range).expect("a whole-keyspace range must be plannable");
		let mut cursor = RangeCursor::new();
		let served = tier.serve(&scan, &whole(CACHED), &mut cursor, 64);

		assert!(!armed.load(Ordering::SeqCst), "the seam hook never fired, so the invariant went unchecked");
		assert!(
			matches!(served, ServedChunk::Gap),
			"a chunk whose claim was retracted mid-walk must be refused, not served from a stale plan"
		);
		assert!(!cursor.is_exhausted(), "a refused chunk must leave the cursor untouched");
	}
}
