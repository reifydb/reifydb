// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Serving a forward range chunk from the interval coverage, the counterpart of the claims
//! [`super::coverage`] publishes.
//!
//! The bucket flag asks "is this whole bucket resident", which no scan starting at a storage prefix can
//! ever answer, and which one commit anywhere in the bucket falsifies for all 65536 of its row numbers. A
//! claim asks the narrower question the scan actually needs: is RAM authoritative from here to there. So a
//! page a commit has just made incomplete still serves everything the claim still holds.
//!
//! Only forward scans are served. `range_rev_next` keeps falling through, deliberately: descending
//! traversal of the interval set is not free and reverse range reads have no measured volume.

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::store::EntryKind;
use reifydb_store::{
	coverage::{Edge, Segment, successor},
	row::page::{PageId, key_range_of, page_of},
};

use crate::{
	MultiVersionScope,
	tier::{
		RangeCursor, RawEntry,
		read::{CoverageOutcome, MultiReadBufferTier, ServedChunk, range::served_chunk},
	},
};

impl MultiReadBufferTier {
	/// Serves the leading run of a forward scan from the interval coverage, over the one page the resume
	/// point falls in.
	///
	/// The claim is the authority and the page is only where the rows live, so this answers spans the
	/// bucket flag cannot: a claim survives a commit into a neighbouring key, where `range_complete` is
	/// cleared for all 65536 row numbers of the bucket at once. What the page still supplies is its key
	/// range, which is where the walk has to stop, so a kind that has none falls through and understates.
	///
	/// A chunk may only report the persistent tier exhausted when the claim reaches past the range end,
	/// because that is the only case in which RAM has proven there is nothing left to find. Every other
	/// stop leaves the cursor on the last row served and the store's loop takes the remainder from
	/// persistent.
	#[allow(clippy::too_many_arguments)]
	pub(super) fn serve_covered_chunk(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		scope: MultiVersionScope,
		batch_size: usize,
		descending: bool,
	) -> ServedChunk {
		if descending {
			return ServedChunk::Gap;
		}
		let range_lo = EncodedKey::new(start);
		let range_hi = EncodedKey::new(end);
		if range_lo > range_hi {
			return ServedChunk::Gap;
		}
		let resume = match &cursor.last_key {
			Some(last) if *last >= range_lo => Some(last.clone()),
			_ => None,
		};
		let lo = match &resume {
			Some(last) => successor(last),
			None => range_lo.clone(),
		};
		if lo > range_hi {
			return ServedChunk::Gap;
		}
		let hi = Edge::Key(successor(&range_hi));

		let shift = self.bucket_shift();
		let anchor = resume.unwrap_or_else(|| range_lo.clone());
		let page = page_of(&anchor, shift);
		let Some((page_start, page_end)) = page_bounds(page, shift) else {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		};
		if lo < page_start {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		}
		let ceiling = Edge::Key(successor(&page_end)).min(hi.clone());

		let Some((planned, token)) = self.plan_leading(table, &lo, &ceiling) else {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		};
		let Some(Segment::Ram(claimed)) = planned.segments.first() else {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		};
		let stop = claimed.end.clone();
		let upper: Bound<EncodedKey> = match &stop {
			Edge::Key(edge) if edge.as_slice() <= page_end.as_slice() => Bound::Excluded(edge.clone()),
			_ => Bound::Included(page_end.clone()),
		};

		let mut out: Vec<RawEntry> = Vec::new();
		let mut full = false;
		{
			let mut shard = self.shard_for(&page).lock();
			let tick = shard.next_tick;
			if let Some(resident) = shard.pages.get_mut(&page) {
				for (key, entry) in resident.entries.range((Bound::Included(lo.clone()), upper)) {
					if out.len() >= batch_size {
						full = true;
						break;
					}
					if scope.contains(entry.version) {
						out.push(RawEntry {
							key: key.clone(),
							version: entry.version,
							value: entry.value.clone(),
						});
					}
				}
				resident.hot = true;
				resident.tick = tick;
				shard.next_tick = tick + 1;
			}
		}

		#[cfg(test)]
		self.interlock(page);

		if self.retractions() != token {
			self.tally_coverage(&anchor, CoverageOutcome::Refused);
			return ServedChunk::Gap;
		}

		let exhausted = !full && stop >= hi;
		if !exhausted && out.is_empty() {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		}
		self.tally_coverage_served(&anchor, out.len() as u64);
		served_chunk(out, cursor, exhausted)
	}

	fn tally_coverage(&self, at: &EncodedKey, outcome: CoverageOutcome) {
		let page = page_of(at, self.bucket_shift());
		let mut shard = self.shard_for(&page).lock();
		match outcome {
			CoverageOutcome::Gap => shard.coverage_metrics.gaps += 1,
			CoverageOutcome::Refused => shard.coverage_metrics.refused += 1,
		}
	}

	fn tally_coverage_served(&self, at: &EncodedKey, rows: u64) {
		let page = page_of(at, self.bucket_shift());
		let mut shard = self.shard_for(&page).lock();
		shard.coverage_metrics.served += 1;
		shard.coverage_metrics.rows += rows;
	}
}

/// The closed key range of a page, or none for a kind whose bucket cannot be turned back into one.
fn page_bounds(page: PageId, shift: u8) -> Option<(EncodedKey, EncodedKey)> {
	match key_range_of(page, shift)? {
		range => match (range.start, range.end) {
			(Bound::Included(start), Bound::Included(end)) => Some((start, end)),
			_ => None,
		},
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::storage::StorageId, store::EntryKind},
		key::{EncodableKey, row::RowKey, series_row::SeriesRowKey},
	};
	use reifydb_store::row::page::{PageId, page_of};
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::row_number::RowNumber};

	use crate::{
		MultiVersionScope,
		tier::{
			RangeCursor, RawEntry,
			read::{MultiReadBufferTier, ReadBufferConfig, ServedChunk},
		},
	};

	const SHIFT: u8 = 4;
	const STORAGE: u64 = 7;
	const BUCKET: u64 = 1 << SHIFT;

	fn config() -> ReadBufferConfig {
		ReadBufferConfig {
			resident_pages: 8,
			resident_bytes: Some(ByteSize::from_gib(1)),
			shards: 1,
			bucket_shift: SHIFT,
		}
	}

	fn tier() -> MultiReadBufferTier {
		MultiReadBufferTier::new(config()).expect("a tier with a byte budget must be constructed")
	}

	fn row(n: u64) -> EncodedKey {
		RowKey {
			storage: StorageId::table(STORAGE),
			row: RowNumber(n),
		}
		.encode()
	}

	fn series(n: u64) -> EncodedKey {
		SeriesRowKey {
			storage: StorageId::table(STORAGE),
			variant_tag: None,
			key: n,
			sequence: 0,
		}
		.encode()
	}

	fn source() -> EntryKind {
		EntryKind::Source(StorageId::table(STORAGE))
	}

	fn entry(n: u64, version: u64) -> RawEntry {
		RawEntry {
			key: row(n),
			version: CommitVersion(version),
			value: Some(CowVec::new(version.to_be_bytes().to_vec())),
		}
	}

	fn page(n: u64) -> PageId {
		page_of(&row(n), SHIFT)
	}

	fn newest() -> MultiVersionScope {
		MultiVersionScope::AsOf {
			read: CommitVersion(u64::MAX),
		}
	}

	fn fill_bucket(read: &MultiReadBufferTier, bucket: u64, rows: &[u64], version: u64) {
		let entries = rows.iter().map(|n| entry(*n, version)).collect();
		read.populate_page(page(bucket * BUCKET), entries, true);
	}

	/// Row keys invert the row number, so the highest row in a bucket is its lowest key: a forward scan
	/// over rows 0..n runs from `row(n)` down to `row(0)`.
	fn serve(
		read: &MultiReadBufferTier,
		cursor: &mut RangeCursor,
		lo_row: u64,
		hi_row: u64,
		batch: usize,
	) -> ServedChunk {
		let start = row(hi_row);
		let end = row(lo_row);
		read.serve_covered_chunk(source(), cursor, start.as_slice(), end.as_slice(), newest(), batch, false)
	}

	fn rows_of(chunk: &ServedChunk) -> Vec<u64> {
		match chunk {
			ServedChunk::Served(batch) => batch
				.entries
				.iter()
				.map(|e| RowKey::decode(&e.key).expect("a served row key must decode").row.0)
				.collect(),
			ServedChunk::Gap => panic!("expected a served chunk, got a gap"),
		}
	}

	fn is_gap(chunk: &ServedChunk) -> bool {
		matches!(chunk, ServedChunk::Gap)
	}

	#[test]
	fn a_claim_serves_a_bucket_the_bucket_flag_no_longer_calls_complete() {
		// The capability this work item exists for. A commit anywhere in a bucket clears range_complete
		// for all of its row numbers, which is what makes the bucket model refuse; the claim only loses
		// the one key that left RAM, so everything either side of it must still be served from the claim.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3, 4, 5], 10);
		read.invalidate(&row(3));

		assert!(!read.page_is_complete(page(0)), "the invalidate must have cleared the bucket flag");

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET - 1, 64);
		assert_eq!(
			rows_of(&chunk),
			vec![5, 4],
			"the claim below the punched key must still serve, where the bucket flag serves nothing"
		);
		assert!(!cursor.exhausted, "a claim that stops at the punched key has proven nothing beyond it");
	}

	#[test]
	fn a_serve_reports_exhausted_only_when_the_claim_reaches_past_the_range_end() {
		// Reporting the persistent tier exhausted is the one thing a serve can say that loses rows. It is
		// only true when RAM has proven there is nothing left in the range, which is when the claim runs
		// past the range's last key and not merely to the last row RAM happens to hold.
		let read = tier();
		fill_bucket(&read, 0, &[2, 4, 6], 10);

		let mut whole = RangeCursor::new();
		let chunk = serve(&read, &mut whole, 0, BUCKET - 1, 64);
		assert_eq!(rows_of(&chunk), vec![6, 4, 2]);
		assert!(whole.exhausted, "a claim spanning the whole range has proven the rest of it empty");

		let punched = tier();
		punched.populate_page(page(0), vec![entry(2, 10), entry(4, 10), entry(6, 10)], true);
		punched.invalidate(&row(1));

		let mut clipped = RangeCursor::new();
		let chunk = serve(&punched, &mut clipped, 0, BUCKET - 1, 64);
		assert_eq!(rows_of(&chunk), vec![6, 4, 2], "the rows below the punched key are the same");
		assert!(
			!clipped.exhausted,
			"the claim now ends at the punched key, so the persistent tier still owes the rest"
		);
	}

	#[test]
	fn a_claim_over_a_bucket_holding_nothing_serves_an_empty_exhausted_chunk() {
		// The proof-of-absence case, and the one a bug makes silent: a warm of an empty span claims it,
		// and the serve must answer "nothing here" rather than fall through, or the claim buys nothing.
		let read = tier();
		read.populate_page(page(0), Vec::new(), true);

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET - 1, 64);
		assert!(rows_of(&chunk).is_empty());
		assert!(cursor.exhausted, "an empty claimed span must terminate the scan, not hand it to persistent");
	}

	#[test]
	fn a_claim_stops_at_its_page_edge_and_leaves_the_rest_of_the_range_to_persistent() {
		// A bucket claim ends one step past the bucket's last key and the next bucket's claim starts
		// below that, so two claims over adjacent buckets never coalesce and a serve can never carry a
		// scan across a page. Reporting exhausted at the page edge would drop the next bucket entirely.
		let read = tier();
		fill_bucket(&read, 1, &[BUCKET + 1, BUCKET + 2], 10);
		fill_bucket(&read, 0, &[1, 2], 10);

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET * 2 - 1, 64);
		assert_eq!(rows_of(&chunk), vec![BUCKET + 2, BUCKET + 1], "only the page the scan resumed in serves");
		assert!(!cursor.exhausted, "the lower bucket is a separate claim the persistent tier still owes");

		cursor.last_key = Some(row(2));
		let next = serve(&read, &mut cursor, 0, BUCKET * 2 - 1, 64);
		assert_eq!(rows_of(&next), vec![1], "resuming inside the lower bucket serves from its own claim");
		assert!(cursor.exhausted, "that claim does reach the range end");
	}

	#[test]
	fn a_batch_limited_serve_leaves_the_cursor_on_its_last_row_and_reports_more() {
		// A chunk that stops on the batch limit must never report exhausted, and must leave the cursor
		// where the next chunk resumes, or the rows past the limit are lost.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3, 4], 10);

		let mut cursor = RangeCursor::new();
		let first = serve(&read, &mut cursor, 0, BUCKET - 1, 2);
		assert_eq!(rows_of(&first), vec![4, 3]);
		assert!(!cursor.exhausted);
		assert_eq!(cursor.last_key.as_ref(), Some(&row(3)));

		let second = serve(&read, &mut cursor, 0, BUCKET - 1, 2);
		assert_eq!(rows_of(&second), vec![2, 1], "the resume must pick up strictly after the last row served");
		assert!(cursor.exhausted);
	}

	#[test]
	fn a_serve_applies_the_version_scope_to_every_entry() {
		// Coverage decides whether a span is claimed; the scope decides which entries in it are visible.
		// Conflating the two would let a reader below a row's version see it.
		let read = tier();
		read.populate_page(page(0), vec![entry(1, 5), entry(2, 50)], true);

		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			row(BUCKET - 1).as_slice(),
			row(0).as_slice(),
			MultiVersionScope::AsOf {
				read: CommitVersion(10),
			},
			64,
			false,
		);
		assert_eq!(rows_of(&chunk), vec![1], "the entry above the read version must be filtered out");
	}

	#[test]
	fn a_kind_whose_page_has_no_reconstructable_range_is_not_served() {
		// Series keys are claimed by a fill like any other, but the walk needs a page's key range to know
		// where the page's responsibility ends. Serving them is a later work item; until then the serve
		// must understate rather than guess a page boundary.
		let read = tier();
		read.insert(series(1), CommitVersion(10), Some(CowVec::new(vec![1])));

		let start = series(0);
		let end = series(9);
		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);
		assert!(is_gap(&chunk), "a page with no key range must fall through, not answer");
		assert!(!cursor.exhausted, "a gap must leave the cursor untouched");
	}

	#[test]
	fn a_reverse_scan_is_never_served_from_a_claim() {
		// Descending traversal of the interval set is not implemented, and a forward walk answering a
		// reverse cursor would return the range in the wrong order.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3], 10);

		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			row(BUCKET - 1).as_slice(),
			row(0).as_slice(),
			newest(),
			64,
			true,
		);
		assert!(is_gap(&chunk));
	}

	#[test]
	fn a_scan_starting_at_a_storage_prefix_is_not_claimed_and_falls_through() {
		// Every range scan in this codebase starts at a ten byte storage prefix, which no claim reaches
		// because a claim's lower end is always a key a fill observed. The first chunk of a scan is
		// therefore always the persistent tier's, and a serve that answered it would be inventing a
		// proof no fill made.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3], 10);

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::storage_end(StorageId::table(STORAGE));
		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);
		assert!(is_gap(&chunk), "no claim covers the prefix the scan starts at");

		cursor.last_key = Some(row(3));
		let resumed = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);
		assert_eq!(rows_of(&resumed), vec![2, 1], "once the cursor is on a real key the claim serves");
	}

	#[test]
	fn a_serve_refuses_a_plan_a_retraction_raced() {
		// The plan is read under the coverage lock and the rows under the page locks, never both, so a
		// removal can land in between and falsify the claim the plan was built from. Serving anyway
		// returns rows RAM no longer holds and, worse, reports proven absence over the key it dropped.
		let read = MultiReadBufferTier::with_interlock(
			config(),
			Box::new(|tier, _page| {
				tier.invalidate(&row(2));
			}),
		)
		.expect("a tier with a byte budget must be constructed");
		fill_bucket(&read, 0, &[1, 2, 3], 10);

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET - 1, 64);
		assert!(is_gap(&chunk), "a plan whose claim was retracted mid-walk must be refused");
		assert!(!cursor.exhausted, "a refused plan must leave the cursor untouched");
		assert!(
			read.shard_metrics().iter().map(|s| s.coverage.refused).sum::<u64>() > 0,
			"the refusal must be counted, or a silent one is indistinguishable from a miss"
		);
	}
}
