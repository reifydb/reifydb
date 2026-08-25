// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;
#[cfg(test)]
use std::sync::atomic::Ordering;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{interface::store::EntryKind, key::row::RowKey};
use reifydb_runtime::sync::rwlock::RwLock;
#[cfg(test)]
use reifydb_store::coverage::interval::Interval;
use reifydb_store::{
	coverage::{
		Edge,
		plan::{DEFAULT_GAP_GUARD, ScanPlan, plan},
		successor,
	},
	row::page::{PageId, key_range_of, page_of},
};

use crate::tier::read::{CoverageIndex, MultiReadBufferTier, Span, scan::page_bounds};

pub(super) fn span_of(range: Option<EncodedKeyRange>) -> Option<Span> {
	let range = range?;
	match (range.start, range.end) {
		(Bound::Included(start), Bound::Included(end)) => Some((start, Edge::Key(successor(&end)))),
		_ => None,
	}
}

pub(super) fn widen(hull: &mut Option<Span>, span: Span) {
	match hull {
		Some((start, end)) => {
			if span.0.as_slice() < start.as_slice() {
				*start = span.0;
			}
			*end = end.clone().max(span.1);
		}
		None => *hull = Some(span),
	}
}

pub(super) fn row_band(kind: EntryKind) -> Option<(EncodedKey, EncodedKey)> {
	match kind {
		EntryKind::Source(storage) => Some((RowKey::storage_start(storage), RowKey::storage_end(storage))),
		_ => None,
	}
}

fn in_row_band(kind: EntryKind, key: &EncodedKey) -> bool {
	match row_band(kind) {
		Some((start, end)) => start.as_slice() <= key.as_slice() && key.as_slice() <= end.as_slice(),
		None => false,
	}
}

pub(super) struct Leading {
	pub(super) lo: EncodedKey,
	pub(super) page: PageId,
	pub(super) page_end: EncodedKey,
	pub(super) plan: ScanPlan,
	pub(super) token: u64,
	pub(super) advanced: bool,
}

fn page_fully_claimed(coverage: &CoverageIndex, page: PageId, shift: u8) -> bool {
	let Some((start, end)) = span_of(key_range_of(page, shift)) else {
		return false;
	};
	coverage.kinds.get(&page.kind).and_then(|set| set.covering(&start)).is_some_and(|claim| claim.end >= end)
}

impl MultiReadBufferTier {
	pub(super) fn coverage(&self) -> &RwLock<CoverageIndex> {
		&self.inner.coverage
	}

	pub(super) fn retractions(&self) -> u64 {
		self.inner.retractions.token()
	}

	pub(super) fn retractions_unchanged(&self, token: u64) -> bool {
		self.inner.retractions.unchanged(token)
	}

	pub(super) fn claims(&self, kind: EntryKind, key: &EncodedKey) -> bool {
		self.coverage().read().kinds.get(&kind).is_some_and(|set| set.contains(key))
	}

	pub fn page_is_complete(&self, page: PageId) -> bool {
		let shift = self.bucket_shift();
		let coverage = self.coverage().read();
		page_fully_claimed(&coverage, page, shift)
	}

	pub(super) fn count_complete_pages(&self, pages: &[PageId]) -> usize {
		let shift = self.bucket_shift();
		let coverage = self.coverage().read();
		pages.iter().filter(|page| page_fully_claimed(&coverage, **page, shift)).count()
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) fn plan_leading(
		&self,
		kind: EntryKind,
		anchor: &EncodedKey,
		lo: &EncodedKey,
		range_hi: &EncodedKey,
		hi: &Edge,
		shift: u8,
		head: bool,
	) -> Option<Leading> {
		let coverage = self.coverage().read();
		let mut lo = lo.clone();
		let mut advanced = false;
		if head && let Some((start, _)) = row_band(kind)
			&& lo.as_slice() >= start.as_slice()
			&& let Some(at) = coverage.heads.get(&kind)
			&& lo.as_slice() < at.as_slice()
			&& at.as_slice() <= range_hi.as_slice()
		{
			lo = at.clone();
			advanced = true;
		}
		let page = page_of(
			if advanced {
				&lo
			} else {
				anchor
			},
			shift,
		);
		let (page_start, page_end) = page_bounds(page, shift)?;
		if lo.as_slice() < page_start.as_slice() {
			return None;
		}
		let ceiling = Edge::Key(successor(&page_end)).min(hi.clone());
		let set = coverage.kinds.get(&kind)?;
		let claim = set.covering(&lo)?;
		let cap = claim.end.min(ceiling);
		let planned = plan(set, lo.clone(), cap, DEFAULT_GAP_GUARD, |_| false);
		Some(Leading {
			lo,
			page,
			page_end,
			plan: planned,
			token: self.retractions(),
			advanced,
		})
	}

	pub(super) fn head_proves_empty(&self, kind: EntryKind, lo: &EncodedKey, range_hi: &EncodedKey) -> bool {
		let Some((start, end)) = row_band(kind) else {
			return false;
		};
		if lo.as_slice() < start.as_slice() {
			return false;
		}
		let coverage = self.coverage().read();
		coverage.heads.get(&kind).is_some_and(|at| {
			at.as_slice() > range_hi.as_slice()
				|| (at.as_slice() == range_hi.as_slice() && range_hi.as_slice() >= end.as_slice())
		})
	}

	pub(super) fn raise_head(
		&self,
		kind: EntryKind,
		lo: &EncodedKey,
		through: &EncodedKey,
		first: Option<&EncodedKey>,
		token: u64,
	) {
		let Some((start, end)) = row_band(kind) else {
			return;
		};
		if lo.as_slice() > start.as_slice() {
			return;
		}
		let proven = match first {
			Some(key) => key.clone(),
			None => successor(through),
		};
		let proven = if proven.as_slice() > end.as_slice() {
			end
		} else {
			proven
		};
		if proven.as_slice() <= start.as_slice() {
			return;
		}
		let mut coverage = self.coverage().write();
		if !self.retractions_unchanged(token) {
			return;
		}
		if coverage.heads.get(&kind).is_none_or(|current| current.as_slice() < proven.as_slice()) {
			coverage.heads.insert(kind, proven);
		}
	}

	pub(super) fn lower_head(&self, kind: EntryKind, key: &EncodedKey) {
		if !in_row_band(kind, key) {
			return;
		}
		{
			let coverage = self.coverage().read();
			if coverage.heads.get(&kind).is_none_or(|current| current.as_slice() <= key.as_slice()) {
				return;
			}
		}
		let mut coverage = self.coverage().write();
		if coverage.heads.get(&kind).is_none_or(|current| current.as_slice() <= key.as_slice()) {
			return;
		}
		coverage.heads.insert(kind, key.clone());
		self.record_retraction();
	}

	pub(super) fn claim(&self, kind: EntryKind, span: &Span, token: u64) -> bool {
		let mut coverage = self.coverage().write();
		if !self.retractions_unchanged(token) {
			#[cfg(test)]
			self.inner.claims_refused.fetch_add(1, Ordering::SeqCst);
			return false;
		}
		coverage.kinds.entry(kind).or_default().extend(span.0.clone(), span.1.clone());
		#[cfg(test)]
		self.inner.claims_published.fetch_add(1, Ordering::SeqCst);
		true
	}

	/// Withdraws one key and moves the retraction counter, whether or not anything was claimed.
	///
	/// A withdrawal that finds nothing must still invalidate every fill already in flight: such a fill
	/// has placed its rows and is about to publish, and the row this withdrawal is about to drop is one
	/// the fill just placed. Skipping the bump leaves the fill's token matching and its claim standing
	/// over a key RAM no longer holds. A bump that refuses a claim only understates, which is free.
	pub(super) fn withdraw_key(&self, kind: EntryKind, key: &EncodedKey) {
		let mut coverage = self.coverage().write();
		let emptied = match coverage.kinds.get_mut(&kind) {
			Some(set) => {
				set.shrink_key(key);
				set.is_empty()
			}
			None => false,
		};
		if emptied {
			coverage.kinds.remove(&kind);
		}
		if in_row_band(kind, key)
			&& coverage.heads.get(&kind).is_some_and(|current| current.as_slice() > key.as_slice())
		{
			coverage.heads.insert(kind, key.clone());
		}
		self.record_retraction();
	}

	pub(super) fn withdraw_span(&self, kind: EntryKind, span: &Span) {
		let mut coverage = self.coverage().write();
		let emptied = match coverage.kinds.get_mut(&kind) {
			Some(set) => {
				set.shrink_range(&span.0, &span.1);
				set.is_empty()
			}
			None => false,
		};
		if emptied {
			coverage.kinds.remove(&kind);
		}
		self.record_retraction();
	}

	pub(super) fn withdraw_all(&self) {
		let mut coverage = self.coverage().write();
		coverage.kinds.clear();
		coverage.heads.clear();
		self.record_retraction();
	}

	fn record_retraction(&self) {
		self.inner.retractions.record()
	}

	#[cfg(test)]
	pub(super) fn claims_published(&self) -> u64 {
		self.inner.claims_published.load(Ordering::SeqCst)
	}

	#[cfg(test)]
	pub(super) fn claims_refused(&self) -> u64 {
		self.inner.claims_refused.load(Ordering::SeqCst)
	}

	#[cfg(test)]
	pub(super) fn drops_refused(&self) -> u64 {
		self.inner.drops_refused.load(Ordering::SeqCst)
	}

	#[cfg(test)]
	pub(super) fn covers(&self, kind: EntryKind, key: &EncodedKey) -> bool {
		self.claims(kind, key)
	}

	#[cfg(test)]
	pub(super) fn head(&self, kind: EntryKind) -> Option<EncodedKey> {
		self.coverage().read().heads.get(&kind).cloned()
	}

	#[cfg(test)]
	pub(super) fn intervals(&self, kind: EntryKind) -> Vec<Interval> {
		self.coverage().read().kinds.get(&kind).map(|set| set.iter().collect()).unwrap_or_default()
	}

	#[cfg(test)]
	pub(super) fn claimed_keys(&self) -> Vec<(EntryKind, Interval)> {
		let coverage = self.coverage().read();
		let mut out = Vec::new();
		for (kind, set) in coverage.kinds.iter() {
			for interval in set.iter() {
				out.push((*kind, interval));
			}
		}
		out
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::storage::StorageId, store::EntryKind},
		key::{EncodableKey, row::RowKey, series_row::SeriesRowKey},
	};
	use reifydb_store::{
		coverage::{Edge, interval::Interval, successor},
		row::page::{PageId, key_range_of, page_of},
	};
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::row_number::RowNumber};

	use crate::tier::{
		RawEntry,
		read::{MultiReadBufferTier, ReadBufferConfig, ResidentPage, coverage::span_of},
	};

	const SHIFT: u8 = 4;
	const STORAGE: u64 = 7;
	const BUCKET: u64 = 1 << SHIFT;

	fn tier(resident_pages: usize) -> MultiReadBufferTier {
		MultiReadBufferTier::new(ReadBufferConfig {
			resident_pages,
			resident_bytes: Some(ByteSize::from_gib(1)),
			shards: 1,
			bucket_shift: SHIFT,
		})
		.expect("a tier with a byte budget must be constructed")
	}

	fn row(n: u64) -> EncodedKey {
		RowKey {
			storage: StorageId::table(STORAGE),
			row: RowNumber(n),
		}
		.encode()
	}

	fn source() -> EntryKind {
		EntryKind::Source(StorageId::table(STORAGE))
	}

	fn val(n: u64) -> CowVec<u8> {
		CowVec::new(n.to_be_bytes().to_vec())
	}

	fn entry(n: u64, version: u64) -> RawEntry {
		RawEntry {
			key: row(n),
			version: CommitVersion(version),
			value: Some(val(version)),
		}
	}

	fn page(n: u64) -> PageId {
		page_of(&row(n), SHIFT)
	}

	fn bucket_span(n: u64) -> (EncodedKey, Edge) {
		span_of(key_range_of(page(n), SHIFT)).expect("a table row page has a reconstructable range")
	}

	fn fill_bucket(read: &MultiReadBufferTier, bucket: u64, rows: &[u64], version: u64) {
		// Rows must be listed descending by row number, which is ascending encoded key order, the order a scan yields them in.
		let base = bucket * BUCKET;
		let entries: Vec<RawEntry> = rows.iter().map(|n| entry(*n, version)).collect();
		assert!(
			read.install_scanned_chunk(source(), &row(base + BUCKET - 1), &row(base), &entries),
			"a whole-bucket chunk must publish its claim"
		);
	}

	fn resident(read: &MultiReadBufferTier, key: &EncodedKey) -> bool {
		let page = page_of(key, SHIFT);
		read.shard_for(&page).lock().pages.get(&page).is_some_and(|page| page.entries.contains_key(key))
	}

	fn island(n: u64) -> Interval {
		Interval::new(row(n), Edge::Key(successor(&row(n))))
	}

	fn storage_start() -> EncodedKey {
		RowKey::storage_start(StorageId::table(STORAGE))
	}

	fn storage_end() -> EncodedKey {
		RowKey::storage_end(StorageId::table(STORAGE))
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

	#[test]
	fn a_single_key_fill_claims_that_key_and_nothing_around_it() {
		// A fill observed one key and nothing about its neighbours; widening would report a row the
		// persistent tier still holds as a proven absence.
		let read = tier(8);

		read.insert(row(4), CommitVersion(1), Some(val(1)));

		assert_eq!(read.intervals(source()), vec![island(4)]);
		assert!(read.covers(source(), &row(4)));
		assert!(!read.covers(source(), &row(3)), "the island widened above its key");
		assert!(!read.covers(source(), &row(5)), "the island widened below its key");
	}

	#[test]
	fn a_single_key_fill_claims_a_kind_with_no_reconstructable_page() {
		// A key whose bucket range cannot be reconstructed can never carry a whole-page claim, so the
		// interval is the only record that can ever make it range-cacheable.
		let read = tier(8);
		let key = EncodedKey::new(b"an-unclassifiable-catalog-key".to_vec());

		read.insert(key.clone(), CommitVersion(1), Some(val(1)));

		assert_eq!(page_of(&key, SHIFT).kind, EntryKind::Multi);
		assert!(read.covers(EntryKind::Multi, &key), "a non-source kind must still be claimable");
		assert!(
			!read.page_is_complete(page_of(&key, SHIFT)),
			"a page with no reconstructable range can never be claimed whole"
		);
	}

	#[test]
	fn a_whole_page_fill_claims_exactly_its_own_bucket() {
		// The claim must be the whole bucket, or a proven-absent row inside it costs a persistent read
		// on every scan; it must be no wider, or it answers for the next bucket's rows.
		let read = tier(8);

		fill_bucket(&read, 0, &[2, 0], 1);

		assert!(read.covers(source(), &row(2)));
		assert!(read.covers(source(), &row(BUCKET - 1)), "a row proven absent inside the bucket");
		assert!(!read.covers(source(), &row(BUCKET)), "the claim reached into the next bucket");
	}

	#[test]
	fn an_invalidate_withdraws_the_claim_before_the_row_leaves_ram() {
		// An invalidate removes the row under the page lock and withdraws the claim under the coverage
		// lock, and between the two it holds neither: every other thread sees whatever order it chose.
		// Withdrawing second leaves a claim standing over a key RAM no longer holds, and a reader that
		// serves that span reports the key absent, so the shrink must come first. The interlock runs at
		// exactly that instant and reads the pair the wrong order would break.
		let overstated = Arc::new(AtomicUsize::new(0));
		let fired = Arc::new(AtomicUsize::new(0));
		let read = {
			let overstated = overstated.clone();
			let fired = fired.clone();
			MultiReadBufferTier::with_invalidate_interlock(
				ReadBufferConfig {
					resident_pages: 8,
					resident_bytes: Some(ByteSize::from_gib(1)),
					shards: 1,
					bucket_shift: SHIFT,
				},
				Box::new(move |read, key| {
					fired.fetch_add(1, Ordering::SeqCst);
					if read.covers(source(), key) && !resident(read, key) {
						overstated.fetch_add(1, Ordering::SeqCst);
					}
				}),
			)
			.expect("a tier with a byte budget must be constructed")
		};

		fill_bucket(&read, 0, &[2, 1, 0], 1);
		read.invalidate(&row(1));

		assert_eq!(fired.load(Ordering::SeqCst), 1, "the interlock never ran, so the window went unread");
		assert!(!resident(&read, &row(1)), "the invalidate left the row in RAM, so nothing was observed");
		assert_eq!(
			overstated.load(Ordering::SeqCst),
			0,
			"the claim outlived the row it covered: a reader in this window serves the span from RAM and \
			 reports a key the persistent tier still holds as absent"
		);
		assert!(read.covers(source(), &row(0)), "the withdrawal took more than the key that left RAM");
		assert!(read.covers(source(), &row(2)), "the withdrawal took more than the key that left RAM");
	}

	#[test]
	fn an_invalidate_punches_out_exactly_the_key_that_left_ram() {
		// A whole-page claim dies for all 2^shift rows on one write; losing the whole span here would
		// throw away the only thing the interval model is for.
		let read = tier(8);
		fill_bucket(&read, 0, &[2, 0], 1);

		read.invalidate(&row(0));

		assert!(!read.covers(source(), &row(0)), "a key RAM no longer holds must not stay claimed");
		assert!(read.covers(source(), &row(1)), "the claim lost more than the key that left RAM");
		assert!(read.covers(source(), &row(2)));
		assert!(!read.page_is_complete(page(0)), "a whole-page claim is still all-or-nothing");
	}

	#[test]
	fn a_page_emptied_by_an_invalidate_leaves_no_claim_behind() {
		// A claim outliving its page answers for a span nothing in RAM can ever be read for again.
		let read = tier(8);
		fill_bucket(&read, 0, &[0], 1);
		assert!(read.covers(source(), &row(1)));

		read.invalidate(&row(0));

		assert_eq!(read.resident_pages(), 0, "the emptied page must leave the tier");
		assert!(read.intervals(source()).is_empty(), "the page left the tier and its claim did not");
	}

	#[test]
	fn one_page_hull_never_reaches_another_page_claim() {
		// The hull retracts a byte span, not a page id. If two pages of one kind shared byte space,
		// evicting either would silently withdraw the other's claim over rows still resident.
		let read = tier(8);
		fill_bucket(&read, 0, &[0], 1);
		fill_bucket(&read, 1, &[BUCKET], 1);
		let hull = read
			.shard_for(&page(0))
			.lock()
			.pages
			.get(&page(0))
			.and_then(|page| page.claimed.clone())
			.expect("a complete fill records a hull");

		read.withdraw_span(page(0).kind, &hull);

		assert!(!read.covers(source(), &row(0)), "the hull did not retract its own page");
		assert!(read.covers(source(), &row(BUCKET)), "one page's hull reached another page's claim");
		assert!(read.covers(source(), &row(BUCKET + 1)));
	}

	#[test]
	fn evicting_a_page_withdraws_every_claim_it_published() {
		// The evicted rows are still in the persistent tier; a surviving claim reports them absent.
		let read = tier(1);
		read.insert(row(0), CommitVersion(1), Some(val(1)));
		read.insert(row(2), CommitVersion(1), Some(val(1)));
		assert_eq!(read.intervals(source()).len(), 2);

		read.insert(row(BUCKET * 4), CommitVersion(1), Some(val(1)));

		assert_eq!(read.resident_pages(), 1, "the page cap must have forced an eviction");
		for probe in [0, 2, BUCKET * 4] {
			assert_eq!(
				read.covers(source(), &row(probe)),
				resident(&read, &row(probe)),
				"a claim survived the eviction of the rows that backed it"
			);
		}
	}

	#[test]
	fn a_drop_refuses_once_a_fill_has_republished_a_claim() {
		// Between the shrink and the drop the two locks are apart; dropping a page a fill has just
		// claimed leaves that claim standing over nothing.
		let read = tier(1);
		read.insert(row(0), CommitVersion(1), Some(val(1)));
		{
			let mut shard = read.shard(0).lock();
			shard.pages.insert(page(BUCKET * 4), ResidentPage::fresh(9));
		}
		let (victim, _, fills) = read.pick_victim(0).expect("two pages against a cap of one");

		assert!(
			!read.drop_victim(0, victim, fills + 1),
			"a drop must refuse when a fill landed since the victim was chosen"
		);
		assert!(read.shard(0).lock().pages.contains_key(&victim), "the refused drop removed the page");
		assert!(read.drop_victim(0, victim, fills), "an unchanged fill count must let the drop through");
		assert!(!read.shard(0).lock().pages.contains_key(&victim));
	}

	#[test]
	fn a_fill_that_read_its_token_before_a_shrink_publishes_nothing() {
		// The fill's rows and its claim land under different locks; without the token the claim
		// reinstates a span the shrink between them has already withdrawn.
		let read = tier(8);
		let span = bucket_span(0);
		let token = read.retractions();

		read.withdraw_all();

		assert!(!read.claim(source(), &span, token), "a claim published across a retraction");
		assert!(!read.covers(source(), &row(3)));
		assert!(read.claim(source(), &span, read.retractions()), "a fresh token must publish");
		assert!(read.covers(source(), &row(3)));
	}

	#[test]
	fn clearing_the_tier_withdraws_every_claim() {
		// Every row goes; a surviving claim reports the whole cleared span as proven absent.
		let read = tier(8);
		fill_bucket(&read, 0, &[0], 1);
		read.insert(row(BUCKET * 4), CommitVersion(1), Some(val(1)));

		read.clear();

		assert!(read.intervals(source()).is_empty());
		assert_eq!(read.resident_pages(), 0);
	}

	#[test]
	fn evicting_the_page_the_head_came_from_leaves_the_head_standing() {
		// Eviction takes rows out of RAM; it cannot put one into the persistent tier. The head asserts
		// only that the persistent tier is empty below it, so it outlives every row that produced it.
		// That is the whole reason it is kept apart from the claims: a claim must die with its page,
		// while a proof of absence that died with its page would be lost on the first page turnover and
		// every scan would fall through at its prefix again.
		let read = tier(1);
		let entries = vec![entry(BUCKET * 4 + 3, 1), entry(BUCKET * 4 + 2, 1), entry(BUCKET * 4 + 1, 1)];
		read.install_scanned_chunk(source(), &storage_start(), &storage_end(), &entries);
		assert_eq!(
			read.head(source()).as_ref(),
			Some(&row(BUCKET * 4 + 3)),
			"the install must have recorded a head"
		);
		assert!(read.covers(source(), &row(BUCKET * 4 + 2)), "the install must have published a claim");

		read.insert(row(1), CommitVersion(1), Some(val(1)));

		assert_eq!(read.resident_pages(), 1, "the page cap must have forced an eviction");
		assert!(!read.covers(source(), &row(BUCKET * 4 + 2)), "the evicted page's claim must be withdrawn");
		assert_eq!(
			read.head(source()).as_ref(),
			Some(&row(BUCKET * 4 + 3)),
			"eviction cannot create a row, so the proof of absence must survive it"
		);
	}

	#[test]
	fn a_row_placed_into_ram_below_the_head_pulls_the_head_back_to_it() {
		// A flush writes a row to the persistent tier and only then seeds it here, so from this call on
		// the persistent tier may hold it. A head left above it makes every later scan begin past the row
		// and never read it from any tier. Placing a row can only ever be evidence that the span below
		// the head is not empty after all, so the head must yield to it, and it must yield before the row
		// lands or a reader in between still skips it.
		let read = tier(8);
		read.raise_head(source(), &storage_start(), &storage_end(), Some(&row(3)), read.retractions());

		read.insert(row(7), CommitVersion(1), Some(val(1)));

		assert_eq!(
			read.head(source()).as_ref(),
			Some(&row(7)),
			"a row placed inside the head span must pull the head back to it"
		);
	}

	#[test]
	fn a_head_raise_that_read_its_token_before_a_withdrawal_publishes_nothing() {
		// The scan that proves a span empty runs under no lock, so a commit can place a row inside that
		// span between the scan and the raise. Publishing the raise anyway makes every later scan start
		// past the new row and never read it from any tier, with no gap and no error to show for it.
		let read = tier(8);
		let token = read.retractions();

		read.invalidate(&row(7));

		read.raise_head(source(), &storage_start(), &storage_end(), Some(&row(3)), token);
		assert_eq!(read.head(source()), None, "a head published across a withdrawal");

		read.raise_head(source(), &storage_start(), &storage_end(), Some(&row(3)), read.retractions());
		assert_eq!(read.head(source()).as_ref(), Some(&row(3)), "a fresh token must publish");
	}

	#[test]
	fn a_scan_below_the_row_band_never_raises_a_head_over_it() {
		// Row keys and series row keys of one storage share an entry kind but occupy disjoint byte bands,
		// with the series band wholly below the row band. A series scan proves nothing about the rows, so
		// a head raised from one would report every row of the storage absent.
		let read = tier(8);

		read.raise_head(source(), &series(9), &storage_end(), Some(&series(1)), read.retractions());

		assert_eq!(read.head(source()), None, "a scan that never entered the row band proved nothing about it");
	}

	struct Lcg(u64);

	impl Lcg {
		fn next(&mut self) -> u64 {
			// A fixed generator keeps a failing seed reproducible; rand's stream is not pinned here.
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			self.0 >> 33
		}
	}

	fn overstated(read: &MultiReadBufferTier, persistent: &BTreeMap<u64, u64>) -> (usize, Option<u64>) {
		let claims = read.claimed_keys();
		let mut checked = 0;
		for (kind, interval) in &claims {
			for number in persistent.keys() {
				let key = row(*number);
				if page_of(&key, SHIFT).kind != *kind || !interval.contains(&key) {
					continue;
				}
				checked += 1;
				if !resident(read, &key) {
					return (checked, Some(*number));
				}
			}
		}
		(checked, None)
	}

	#[test]
	fn randomised_fills_evictions_and_invalidates_never_overstate_coverage() {
		// A claim says RAM answers for its whole span, so every row the persistent tier holds inside a
		// claimed interval must be resident. A claim standing over a row RAM dropped is silent wrong
		// data the moment a read is served from it. Understating is legal and never fails here.
		//
		// The model mirrors the production write paths: a commit invalidates while the persistent tier
		// still holds the old row, the sweep writes persistent and then inserts, a rejected sweep
		// invalidates and leaves persistent alone, and an install publishes exactly what persistent holds.
		const ROWS: u64 = BUCKET * 4;
		let mut total_checked = 0usize;
		let mut evictions = 0usize;
		for seed in [1u64, 7, 13, 29, 101] {
			let read = tier(2);
			let mut rng = Lcg(seed);
			let mut persistent: BTreeMap<u64, u64> = BTreeMap::new();
			let mut pending: BTreeMap<u64, u64> = BTreeMap::new();
			let mut version = 1u64;
			for step in 0..400 {
				let number = rng.next() % ROWS;
				match rng.next() % 5 {
					0 => {
						version += 1;
						pending.insert(number, version);
						read.invalidate(&row(number));
					}
					1 => {
						if let Some(at) = pending.remove(&number) {
							persistent.insert(number, at);
							read.insert(row(number), CommitVersion(at), Some(val(at)));
						}
					}
					2 => {
						pending.remove(&number);
						read.invalidate(&row(number));
					}
					3 => {
						let target = page(number);
						let base = number & !(BUCKET - 1);
						let entries: Vec<RawEntry> = persistent
							.iter()
							.filter(|(at, _)| page(**at) == target)
							.map(|(at, version)| entry(*at, *version))
							.rev()
							.collect();
						read.install_scanned_chunk(
							source(),
							&row(base + BUCKET - 1),
							&row(base),
							&entries,
						);
					}
					_ => {
						let before = read.resident_pages();
						read.evict_to_capacity(0);
						evictions += before - read.resident_pages();
					}
				}
				let (checked, missing) = overstated(&read, &persistent);
				total_checked += checked;
				assert_eq!(
					missing, None,
					"seed {seed} step {step}: a claim covers a persistent row RAM does not hold"
				);
			}
			evictions += read.resident_pages();
		}
		assert!(total_checked > 0, "no claim ever covered a persistent row, so nothing was asserted");
		assert!(evictions > 0, "no page was ever evicted, so the retraction path was never exercised");
	}

	#[test]
	fn a_fill_publishes_nothing_when_an_eviction_drops_its_page_between_the_rows_and_the_claim() {
		// The rows and the claim of one fill land under two locks. The interlock retracts and drops the
		// page in the window between them, which is the interleaving the retraction token exists for;
		// without it the claim reinstates a span whose page is already gone and nothing withdraws it.
		let fired = Arc::new(AtomicUsize::new(0));
		let read = {
			let fired = fired.clone();
			MultiReadBufferTier::with_interlock(
				ReadBufferConfig {
					resident_pages: 8,
					resident_bytes: Some(ByteSize::from_gib(1)),
					shards: 1,
					bucket_shift: SHIFT,
				},
				Box::new(move |read, victim| {
					if fired.fetch_add(1, Ordering::Relaxed) != 0 {
						return;
					}
					let (hull, fills) = {
						let shard = read.shard(0).lock();
						let page = shard.pages.get(&victim).expect("the fill placed rows");
						(page.claimed.clone(), page.fills)
					};
					read.withdraw_span(victim.kind, &hull.expect("the fill recorded a hull"));
					assert!(
						read.drop_victim(0, victim, fills),
						"the interlock must drop its victim"
					);
				}),
			)
			.expect("a tier with a byte budget must be constructed")
		};

		read.insert(row(0), CommitVersion(1), Some(val(1)));

		assert_eq!(fired.load(Ordering::Relaxed), 1, "the interlock must have run inside the fill");
		assert!(!resident(&read, &row(0)), "the interlock did not drop the page it was handed");
		assert!(
			read.intervals(source()).is_empty(),
			"a fill published a claim over the page the eviction in its window had dropped"
		);
	}
}
