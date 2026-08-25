// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::atomic::Ordering};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::interface::store::EntryKind;
use reifydb_runtime::sync::rwlock::RwLock;
#[cfg(test)]
use reifydb_store::coverage::interval::Interval;
use reifydb_store::{
	coverage::{
		Edge,
		plan::{DEFAULT_GAP_GUARD, ScanPlan, plan},
		successor,
	},
	row::page::{PageId, key_range_of},
};

use crate::tier::read::{CoverageIndex, MultiReadBufferTier, Span};

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
		self.inner.retractions.load(Ordering::SeqCst)
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

	pub(super) fn plan_leading(&self, kind: EntryKind, lo: &EncodedKey, hi: &Edge) -> Option<(ScanPlan, u64)> {
		let coverage = self.coverage().read();
		let set = coverage.kinds.get(&kind)?;
		let claim = set.covering(lo)?;
		let cap = claim.end.min(hi.clone());
		let planned = plan(set, lo.clone(), cap, DEFAULT_GAP_GUARD, |_| false);
		Some((planned, self.retractions()))
	}

	pub(super) fn claim(&self, kind: EntryKind, span: &Span, token: u64) -> bool {
		let mut coverage = self.coverage().write();
		if self.retractions() != token {
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
		self.record_retraction();
	}

	fn record_retraction(&self) {
		self.inner.retractions.fetch_add(1, Ordering::SeqCst);
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
		key::{EncodableKey, row::RowKey},
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

	fn resident(read: &MultiReadBufferTier, key: &EncodedKey) -> bool {
		let page = page_of(key, SHIFT);
		read.shard_for(&page).lock().pages.get(&page).is_some_and(|page| page.entries.contains_key(key))
	}

	fn island(n: u64) -> Interval {
		Interval::new(row(n), Edge::Key(successor(&row(n))))
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

		read.populate_page(page(0), vec![entry(0, 1), entry(2, 1)], true);

		assert!(read.covers(source(), &row(2)));
		assert!(read.covers(source(), &row(BUCKET - 1)), "a row proven absent inside the bucket");
		assert!(!read.covers(source(), &row(BUCKET)), "the claim reached into the next bucket");
	}

	#[test]
	fn a_page_fill_that_is_not_complete_claims_nothing() {
		// Rows placed without a proof that they are all of them say nothing about the span between.
		let read = tier(8);

		read.populate_page(page(0), vec![entry(0, 1), entry(2, 1)], false);

		assert!(read.intervals(source()).is_empty(), "an incomplete fill claimed a span it never proved");
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

		read.populate_page(page(0), vec![entry(0, 1), entry(1, 1), entry(2, 1)], true);
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
		read.populate_page(page(0), vec![entry(0, 1), entry(2, 1)], true);

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
		read.populate_page(page(0), vec![entry(0, 1)], true);
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
		read.populate_page(page(0), vec![entry(0, 1)], true);
		read.populate_page(page(BUCKET), vec![entry(BUCKET, 1)], true);
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
		read.populate_page(page(0), vec![entry(0, 1)], true);
		read.insert(row(BUCKET * 4), CommitVersion(1), Some(val(1)));

		read.clear();

		assert!(read.intervals(source()).is_empty());
		assert_eq!(read.resident_pages(), 0);
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
						let entries: Vec<RawEntry> = persistent
							.iter()
							.filter(|(at, _)| page(**at) == target)
							.map(|(at, version)| entry(*at, *version))
							.collect();
						read.populate_page(target, entries, true);
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
