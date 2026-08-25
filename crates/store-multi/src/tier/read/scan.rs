// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::store::EntryKind, key::row::RowKey};
use reifydb_store::{
	coverage::{Edge, plan::Segment, successor},
	row::page::{PageId, key_range_of, page_of},
};

use crate::{
	MultiVersionScope,
	tier::{
		RangeCursor, RawEntry,
		read::{CoverageOutcome, MultiReadBufferTier, ServedChunk, coverage::Leading, range::served_chunk},
	},
};

impl MultiReadBufferTier {
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
		let head = resume.is_none();
		let anchor = resume.unwrap_or_else(|| range_lo.clone());
		let Some(Leading {
			lo,
			page,
			page_end,
			plan: planned,
			token,
			advanced,
		}) = self.plan_leading(table, &anchor, &lo, &range_hi, &hi, shift, head)
		else {
			if self.head_proves_empty(table, &lo, &range_hi) {
				self.tally_coverage_served(&anchor, 0);
				return served_chunk(Vec::new(), cursor, true);
			}
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		};
		if advanced {
			self.tally_head_advance(page);
		}
		let Some(Segment::Resident(claimed)) = planned.segments.first() else {
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

		if !self.retractions_unchanged(token) {
			self.tally_coverage(&anchor, CoverageOutcome::Refused);
			return ServedChunk::Gap;
		}

		let exhausted = !full
			&& (stop >= hi
				|| (stop >= Edge::Key(successor(&page_end))
					&& page_ends_the_range(table, page, &range_hi)));
		if !exhausted && out.is_empty() {
			self.tally_coverage(&anchor, CoverageOutcome::Gap);
			return ServedChunk::Gap;
		}
		self.tally_coverage_served(&anchor, out.len() as u64);
		served_chunk(out, cursor, exhausted)
	}

	fn tally_head_advance(&self, page: PageId) {
		let mut shard = self.shard_for(&page).lock();
		shard.coverage_metrics.head_advances += 1;
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

fn page_ends_the_range(table: EntryKind, page: PageId, range_hi: &EncodedKey) -> bool {
	let EntryKind::Source(storage) = table else {
		return false;
	};
	if page.kind != table || page.series || page.bucket != 0 {
		return false;
	}
	range_hi.as_slice() <= RowKey::storage_end(storage).as_slice()
}

pub(super) fn page_bounds(page: PageId, shift: u8) -> Option<(EncodedKey, EncodedKey)> {
	let range = key_range_of(page, shift)?;
	match (range.start, range.end) {
		(Bound::Included(start), Bound::Included(end)) => Some((start, end)),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	};

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
		// A scan yields entries in ascending key order, which row keys invert into descending row number, so
		// the caller's order cannot be trusted.
		let base = bucket * BUCKET;
		let mut entries: Vec<RawEntry> = rows.iter().map(|n| entry(*n, version)).collect();
		entries.sort_by(|left, right| left.key.cmp(&right.key));
		assert!(
			read.install_scanned_chunk(source(), &row(base + BUCKET - 1), &row(base), &entries),
			"a whole-bucket chunk must publish its claim"
		);
	}

	fn storage_start() -> EncodedKey {
		RowKey::storage_start(StorageId::table(STORAGE))
	}

	fn storage_end() -> EncodedKey {
		RowKey::storage_end(StorageId::table(STORAGE))
	}

	/// Installs a chunk of a scan that began at the storage prefix and ran to the storage end, which is
	/// the shape of every full scan in this codebase; `rows` must be listed in encoded key order, so
	/// descending by row number.
	fn install_from_prefix(read: &MultiReadBufferTier, rows: &[u64], version: u64) {
		let entries: Vec<RawEntry> = rows.iter().map(|n| entry(*n, version)).collect();
		read.install_scanned_chunk(source(), &storage_start(), &storage_end(), &entries);
	}

	fn serve_whole_storage(read: &MultiReadBufferTier, cursor: &mut RangeCursor) -> ServedChunk {
		read.serve_covered_chunk(
			source(),
			cursor,
			storage_start().as_slice(),
			storage_end().as_slice(),
			newest(),
			64,
			false,
		)
	}

	fn head_advances(read: &MultiReadBufferTier) -> u64 {
		read.shard_metrics().iter().map(|shard| shard.coverage.head_advances).sum()
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
	fn a_claim_serves_a_bucket_no_longer_covered_end_to_end() {
		// A commit anywhere in a bucket withdraws the whole-page claim covering all of its row numbers;
		// the interval only loses the one key that left RAM, so everything either side of it must still
		// be served from the claim.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3, 4, 5], 10);
		read.invalidate(&row(3));

		assert!(!read.page_is_complete(page(0)), "the invalidate must have withdrawn the whole-page claim");

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET - 1, 64);
		assert_eq!(
			rows_of(&chunk),
			vec![5, 4],
			"the claim below the punched key must still serve, where a whole-page claim serves nothing"
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
		fill_bucket(&punched, 0, &[2, 4, 6], 10);
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
		// The proof-of-absence case, and the one a bug makes silent: an install of an empty span claims it,
		// and the serve must answer "nothing here" rather than fall through, or the claim buys nothing.
		let read = tier();
		fill_bucket(&read, 0, &[], 10);

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
		assert!(
			read.install_scanned_chunk(source(), &row(BUCKET - 1), &row(0), &[entry(2, 50), entry(1, 5)]),
			"the bucket chunk must publish its claim"
		);

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
	fn a_claim_over_the_last_bucket_reports_exhausted_at_the_storage_end() {
		// Every scan ends at a storage end sentinel no bucket claim can ever reach, so without a tail rule the
		// last chunk of every scan falls through and buys one persistent read to confirm the range is over.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3], 10);

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::storage_end(StorageId::table(STORAGE));
		let mut cursor = RangeCursor::new();
		cursor.last_key = Some(row(3));
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(cursor.exhausted, "no row key of this storage can sort above the claim over its last bucket");
		assert_eq!(rows_of(&chunk), vec![2, 1]);
	}

	#[test]
	fn a_last_bucket_claim_punched_short_of_the_page_end_is_not_exhausted() {
		// The tail rule needs one claim spanning the whole last page; a claim ending at a punched key proves
		// nothing past it and reporting exhausted there silently drops every remaining row.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3], 10);
		read.invalidate(&row(1));

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::storage_end(StorageId::table(STORAGE));
		let mut cursor = RangeCursor::new();
		cursor.last_key = Some(row(3));
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(!cursor.exhausted, "the claim stops at the punched key, which proves nothing past it");
		assert_eq!(rows_of(&chunk), vec![2]);
	}

	#[test]
	fn a_last_bucket_claim_punched_at_the_final_row_is_not_exhausted() {
		// A commit into row zero ends the claim exactly at the page end rather than past it, so the tail rule
		// must compare against the successor or it reports the byte-highest row of the storage proven absent.
		let read = tier();
		fill_bucket(&read, 0, &[0, 1, 2, 3], 10);
		read.invalidate(&row(0));

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::storage_end(StorageId::table(STORAGE));
		let mut cursor = RangeCursor::new();
		cursor.last_key = Some(row(3));
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(!cursor.exhausted, "the claim stops one key short of the page end, which still holds a row");
		assert_eq!(rows_of(&chunk), vec![2, 1]);
	}

	#[test]
	fn a_claim_over_a_bucket_that_is_not_the_last_is_not_exhausted_at_the_storage_end() {
		// Every scan ends at the storage end, so a tail rule keyed on the range rather than on the page being
		// the storage's last would report exhausted on the first bucket served to its edge and drop every
		// bucket below it.
		let read = tier();
		fill_bucket(&read, 1, &[BUCKET + 1, BUCKET + 2], 10);
		fill_bucket(&read, 0, &[1, 2], 10);

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::storage_end(StorageId::table(STORAGE));
		let mut cursor = RangeCursor::new();
		cursor.last_key = Some(row(BUCKET + 2));
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(!cursor.exhausted, "the lower bucket is a separate claim the persistent tier still owes");
		assert_eq!(rows_of(&chunk), vec![BUCKET + 1]);
	}

	#[test]
	fn a_range_reaching_past_the_storage_end_is_never_reported_exhausted() {
		// A range is classified by its start, so its end may lie in another storage whose rows this claim says
		// nothing about; reporting exhausted there drops all of them.
		let read = tier();
		fill_bucket(&read, 0, &[1, 2, 3], 10);

		let start = RowKey::storage_start(StorageId::table(STORAGE));
		let end = RowKey::encoded(StorageId::table(STORAGE - 1), 5);
		let mut cursor = RangeCursor::new();
		cursor.last_key = Some(row(3));
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			newest(),
			64,
			false,
		);

		assert!(!cursor.exhausted, "the claim says nothing about the storage the range runs on into");
		assert_eq!(rows_of(&chunk), vec![2, 1]);
		assert!(
			end.as_slice() > RowKey::storage_end(StorageId::table(STORAGE)).as_slice(),
			"the range end must really sort past this storage, or the case under test never arose"
		);
	}

	#[test]
	fn a_scan_starting_at_a_storage_prefix_serves_once_the_head_names_the_first_row() {
		// A scan starts at a ten byte storage prefix that sorts below every key of the storage, so no
		// claim can ever reach it and the leading chunk of every scan falls through. Where scans are one
		// chunk long that is every chunk, and the read tier answers nothing at all. One recorded key
		// proving the span below the first row empty is enough to move the scan onto a page a claim does
		// cover, and it is the only thing that can be: where the first row lies cannot be derived, only
		// observed.
		let read = tier();
		install_from_prefix(&read, &[3, 2, 1], 10);

		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&read, &mut cursor);

		assert_eq!(rows_of(&chunk), vec![3, 2, 1], "the leading chunk of a prefix scan must serve from RAM");
		assert!(cursor.exhausted, "the claim reaches the storage end, so nothing is left for persistent");
		assert_eq!(
			head_advances(&read),
			1,
			"the serve must be attributed to the head, not to a claim over the prefix"
		);
	}

	#[test]
	fn a_commit_below_the_head_pulls_it_back_and_stops_the_scan_skipping_the_new_row() {
		// The head proves the persistent tier holds nothing below it. A commit places a row inside that
		// span, so a head left standing makes every later scan begin past the new row. That loss is
		// silent: the chunk is served, not gapped, and reports the range exhausted, so the row is never
		// read from any tier.
		let read = tier();
		install_from_prefix(&read, &[3, 2, 1], 10);
		assert_eq!(read.head(source()).as_ref(), Some(&row(3)), "the install must have recorded a head");

		read.invalidate(&row(7));

		assert_eq!(
			read.head(source()).as_ref(),
			Some(&row(7)),
			"a row committed inside the head span must pull the head back to it"
		);
		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&read, &mut cursor);
		assert!(
			is_gap(&chunk),
			"the span the commit landed in is no longer claimed, so the scan must fall through"
		);
		assert!(!cursor.exhausted, "a gap must leave the cursor untouched");
	}

	#[test]
	fn the_head_never_moves_a_scan_past_the_end_of_its_own_range() {
		// The head names the first row of the whole storage, which can sort past the end of a narrower
		// range. Moving lo there abandons the span the caller asked about and consults a claim over a
		// span it did not, so a range RAM can prove empty falls through to the persistent tier instead.
		let read = tier();
		install_from_prefix(&read, &[3, 2, 1], 10);
		assert_eq!(read.head(source()).as_ref(), Some(&row(3)), "the install must have recorded a head");

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 5, 9, 64);

		assert!(rows_of(&chunk).is_empty(), "no row of this storage lies in rows five through nine");
		assert!(cursor.exhausted, "the claim spans the whole range, so RAM has proven it empty");
		assert_eq!(head_advances(&read), 0, "the head sorts past this range and must not have been used");
	}

	#[test]
	fn a_head_advanced_serve_is_refused_when_a_retraction_races_it() {
		// The head and the claim plan are read under one hold of the coverage lock; the rows are read
		// under the page lock afterwards, with neither held in between. A withdrawal landing in that
		// window falsifies both, and serving anyway reports proven absence over the key it removed.
		let armed = Arc::new(AtomicBool::new(false));
		let read = {
			let armed = armed.clone();
			MultiReadBufferTier::with_interlock(
				config(),
				Box::new(move |tier, _page| {
					if armed.load(Ordering::SeqCst) {
						tier.invalidate(&row(2));
					}
				}),
			)
			.expect("a tier with a byte budget must be constructed")
		};
		install_from_prefix(&read, &[3, 2, 1], 10);
		armed.store(true, Ordering::SeqCst);

		let mut cursor = RangeCursor::new();
		let chunk = serve_whole_storage(&read, &mut cursor);

		assert_eq!(head_advances(&read), 1, "the head must have moved lo, or the race under test never arose");
		assert!(is_gap(&chunk), "a chunk whose head and claim were retracted mid-walk must be refused");
		assert!(!cursor.exhausted, "a refused chunk must leave the cursor untouched");
		assert!(
			read.shard_metrics().iter().map(|s| s.coverage.refused).sum::<u64>() > 0,
			"the refusal must be counted, or a silent one is indistinguishable from a miss"
		);
	}

	#[test]
	fn a_range_below_the_row_band_is_never_moved_onto_it_by_the_head() {
		// One entry kind covers both a storage's row keys and its series row keys, and the two bands are
		// disjoint: they differ in their leading kind byte and the series band sorts wholly below the row
		// band. A head names a row key, so applying it to a range starting below that band moves the scan
		// off the keys the caller asked for and onto the rows, reporting everything below proven absent.
		let read = tier();
		install_from_prefix(&read, &[3, 2, 1], 10);
		read.insert(series(1), CommitVersion(10), Some(CowVec::new(vec![1])));
		assert!(
			series(1).as_slice() < storage_start().as_slice(),
			"the series band must sort below the row band, or this range never crosses the boundary"
		);

		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			series(9).as_slice(),
			storage_end().as_slice(),
			newest(),
			64,
			false,
		);

		assert!(is_gap(&chunk), "a range starting below the row band must never be answered from a row head");
		assert!(!cursor.exhausted, "a gap must leave the cursor untouched");
		assert_eq!(head_advances(&read), 0, "the head must not have been applied outside its own band");
	}

	#[test]
	fn a_serve_refuses_a_plan_a_retraction_raced() {
		// The plan is read under the coverage lock and the rows under the page locks, never both, so a
		// removal can land in between and falsify the claim the plan was built from. Serving anyway
		// returns rows RAM no longer holds and, worse, reports proven absence over the key it dropped.
		// The interlock stays disarmed while the bucket is installed, because an install is a fill too and
		// would race its own seeding.
		let armed = Arc::new(AtomicBool::new(false));
		let read = MultiReadBufferTier::with_interlock(config(), {
			let armed = armed.clone();
			Box::new(move |tier, _page| {
				if armed.load(Ordering::SeqCst) {
					tier.invalidate(&row(2));
				}
			})
		})
		.expect("a tier with a byte budget must be constructed");
		fill_bucket(&read, 0, &[1, 2, 3], 10);
		armed.store(true, Ordering::SeqCst);

		let mut cursor = RangeCursor::new();
		let chunk = serve(&read, &mut cursor, 0, BUCKET - 1, 64);
		assert!(is_gap(&chunk), "a plan whose claim was retracted mid-walk must be refused");
		assert!(!cursor.exhausted, "a refused plan must leave the cursor untouched");
		assert!(
			read.shard_metrics().iter().map(|s| s.coverage.refused).sum::<u64>() > 0,
			"the refusal must be counted, or a silent one is indistinguishable from a miss"
		);
	}

	#[test]
	fn an_empty_storage_is_read_from_persistent_once_and_never_again() {
		// Neither storage sentinel resolves to a row page, so the head is the only proof an empty storage can
		// ever produce; without cashing it in every scan falls through to persistent forever.
		let read = tier();

		let mut first = RangeCursor::new();
		assert!(is_gap(&serve_whole_storage(&read, &mut first)), "nothing is proven before the first scan");

		read.install_scanned_chunk(source(), &storage_start(), &storage_end(), &[]);

		let mut second = RangeCursor::new();
		let chunk = serve_whole_storage(&read, &mut second);
		assert!(!is_gap(&chunk), "the proven-empty storage must never reach the persistent tier again");
		assert!(rows_of(&chunk).is_empty(), "a proven-empty range must serve no rows");
		assert!(
			second.exhausted,
			"an empty range that is not exhausted hands the scan straight back to persistent"
		);
	}

	#[test]
	fn a_range_ending_on_the_head_is_never_answered_empty() {
		// The head names a key a row may sit on, so only the storage end sentinel, which no row can occupy, may
		// be answered as proven empty; answering at the head itself drops the row standing on it.
		let read = tier();
		install_from_prefix(&read, &[5, 3], 10);
		assert_eq!(
			read.head(source()).as_ref(),
			Some(&row(5)),
			"the install must name the first row as the head"
		);

		read.invalidate(&row(5));
		read.invalidate(&row(3));

		let mut cursor = RangeCursor::new();
		let chunk = read.serve_covered_chunk(
			source(),
			&mut cursor,
			storage_start().as_slice(),
			row(5).as_slice(),
			newest(),
			64,
			false,
		);
		assert!(
			is_gap(&chunk),
			"a range whose last key is the head itself is not proven empty and the persistent tier still owes it"
		);
		assert!(!cursor.exhausted, "a gap must leave the cursor untouched");
	}
}
