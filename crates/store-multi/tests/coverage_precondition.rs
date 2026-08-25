// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Coverage asserts "no key exists strictly between a and b", which in an MVCC store is only true as
//! of a version. These tests pin the property that makes a version stamp unnecessary: every write path
//! leaves the key reachable from RAM or the always-scanned commit buffer before the span can be claimed
//! covered, so a reader at a version newer than the claim can never miss it.

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::MultiVersionCommit,
	},
	key::row::RowKey,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

const STORAGE: StorageId = StorageId::Table(TableId(1));

/// The default shift of 16 keeps rows 0..65535 in bucket 0, so every row seeded here lands in one page and
/// a full scan of it installs that page's coverage from the chunks it already read.
const BUCKET_ROWS: u64 = 200;

struct StaticWatermark(CommitVersion);

impl EvictionWatermark for StaticWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

fn store() -> (StandardMultiStore, impl Drop) {
	StandardMultiStore::testing_memory_with_persistent_sqlite()
}

fn commit_set(store: &StandardMultiStore, row: u64, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: RowKey::encoded(STORAGE, row),
			bytes: EncodedBytes(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

fn commit_remove(store: &StandardMultiStore, row: u64, version: u64) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::remove_silent(RowKey::encoded(STORAGE, row))],
		CommitVersion(version),
	)
	.unwrap();
}

/// Drives the genuine flush engine sweep, not a stand-in, so the persist -> refresh-read-tier -> drop-from-commit
/// ordering under test is the one production runs.
fn flush(store: &StandardMultiStore, cutoff: u64) {
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(cutoff))));
	store.flush_pending_blocking();
}

fn scan(store: &StandardMultiStore, read: u64) -> BTreeMap<Vec<u8>, (Vec<u8>, CommitVersion)> {
	store.range(
		RowKey::full_scan(STORAGE),
		MultiVersionScope::AsOf {
			read: CommitVersion(read),
		},
		64,
	)
	.map(|r| r.unwrap())
	.map(|r| (r.key.to_vec(), (r.bytes.to_vec(), r.version)))
	.collect()
}

fn complete_pages(store: &StandardMultiStore) -> usize {
	store.read_buffer_shard_metrics().iter().map(|s| s.state.complete_pages).sum()
}

fn scan_between(store: &StandardMultiStore, after: u64, read: u64) -> BTreeMap<Vec<u8>, (Vec<u8>, CommitVersion)> {
	store.range(
		RowKey::full_scan(STORAGE),
		MultiVersionScope::Between {
			after: CommitVersion(after),
			read: CommitVersion(read),
		},
		64,
	)
	.map(|r| r.unwrap())
	.map(|r| (r.key.to_vec(), (r.bytes.to_vec(), r.version)))
	.collect()
}

fn installs(store: &StandardMultiStore) -> u64 {
	store.read_buffer_shard_metrics().iter().map(|s| s.coverage.installs).sum()
}

fn range_served(store: &StandardMultiStore) -> u64 {
	store.read_buffer_shard_metrics().iter().map(|s| s.reads.range_served).sum()
}

fn key(row: u64) -> Vec<u8> {
	RowKey::encoded(STORAGE, row).to_vec()
}

/// Seeds only even rows so an odd row number is a brand-new key that lands in the middle of the encoded
/// scan order rather than in the first persistent chunk, which is what makes the RAM serve responsible for
/// it. A key the very first persistent chunk happens to cover proves nothing about a claimed span.
fn seed(store: &StandardMultiStore, version: u64) {
	for n in 1..=BUCKET_ROWS {
		commit_set(store, n * 2, version, &format!("v{n}"));
	}
}

const MID_ROW: u64 = BUCKET_ROWS - 1;

const MID_EXISTING_ROW: u64 = BUCKET_ROWS;

/// Scans the bucket and asserts the chunks it read actually published their span, so a later assertion
/// about a claimed span is not vacuously true because nothing was ever claimed.
fn scan_and_require_claimed(store: &StandardMultiStore, read: u64) {
	let before = installs(store);
	let _ = scan(store, read);
	assert!(installs(store) > before, "the scan must have installed at least one claim from its own chunks");
	assert!(complete_pages(store) > 0, "the scan must have claimed at least one page complete");
}

#[test]
fn a_commit_into_a_claimed_span_is_visible_at_the_write_version() {
	// The plain writer-newer-than-the-claim case: page P is claimed complete at version 1, then key MID_ROW
	// is committed into P's span at version 20. A reader at 20 must see it. The claim was proved at 1, so
	// trusting it blindly would drop the key; only the always-scanned commit buffer makes it visible.
	let (store, _g) = store();
	seed(&store, 1);
	flush(&store, 1);
	scan_and_require_claimed(&store, 10);

	let before = range_served(&store);
	commit_set(&store, MID_ROW, 20, "written-after-the-claim");

	let rows = scan(&store, 20);
	assert_eq!(
		rows.get(&key(MID_ROW)).map(|(v, ver)| (v.clone(), *ver)),
		Some((b"written-after-the-claim".to_vec(), CommitVersion(20))),
		"a key written into a span claimed at an older version must still reach a reader at the write version"
	);
	assert_eq!(rows.len(), BUCKET_ROWS as usize + 1, "the claimed span's own keys must survive the write");
	assert!(range_served(&store) > before, "the read tier must actually have served, or this proves nothing");
}

#[test]
fn a_flush_of_a_key_committed_after_a_claim_leaves_it_in_ram() {
	// The dangerous interleaving. Key MID_ROW is committed at version 20 while page P is cold, then a scan
	// re-claims P from the persistent tier, which does not hold MID_ROW yet, so P is claimed complete WITHOUT
	// it and only the commit buffer carries it. The flush then moves MID_ROW out of the commit buffer. If the
	// sweep does not place MID_ROW into RAM before dropping it from the commit buffer, the claimed span is
	// served from RAM and MID_ROW vanishes even though the persistent tier holds it.
	let (store, _g) = store();
	seed(&store, 1);
	flush(&store, 1);
	scan_and_require_claimed(&store, 10);

	commit_set(&store, MID_ROW, 20, "committed-then-flushed");
	assert_eq!(complete_pages(&store), 0, "the commit must have dropped the claim on its own page");

	let rescanned = scan(&store, 20);
	assert!(
		rescanned.contains_key(&key(MID_ROW)),
		"the commit buffer must carry the key while it is uncommitted to disk"
	);
	assert!(complete_pages(&store) > 0, "the re-scan must reclaim the page, or the interleaving is not exercised");

	flush(&store, 20);

	let served_before = range_served(&store);
	let after = scan(&store, 20);
	assert!(range_served(&store) > served_before, "the claimed page must actually serve, or this proves nothing");
	assert_eq!(
		after.get(&key(MID_ROW)).map(|(v, ver)| (v.clone(), *ver)),
		Some((b"committed-then-flushed".to_vec(), CommitVersion(20))),
		"a key the sweep moved out of the commit buffer must be reachable from RAM, otherwise a claimed span silently drops it"
	);
	assert_eq!(after.len(), BUCKET_ROWS as usize + 1, "no other key may be lost by the sweep");
}

#[test]
fn a_remove_of_a_key_inside_a_claimed_span_is_visible_at_the_remove_version() {
	// Same hazard in the deletion direction: a tombstone written into a claimed span must reach a reader at
	// the tombstone's version, both while it sits in the commit buffer and after the sweep has moved it out.
	let (store, _g) = store();
	seed(&store, 1);
	flush(&store, 1);
	scan_and_require_claimed(&store, 10);

	commit_remove(&store, MID_EXISTING_ROW, 20);
	let buffered = scan(&store, 20);
	assert!(
		!buffered.contains_key(&key(MID_EXISTING_ROW)),
		"the tombstone must hide the row while it sits in the commit buffer"
	);

	let rescanned = scan(&store, 20);
	assert!(complete_pages(&store) > 0, "the re-scan must reclaim the page, or the interleaving is not exercised");
	assert!(
		!rescanned.contains_key(&key(MID_EXISTING_ROW)),
		"reclaiming the page must not resurrect the removed row"
	);

	flush(&store, 20);
	let after = scan(&store, 20);
	assert!(
		!after.contains_key(&key(MID_EXISTING_ROW)),
		"the swept tombstone must not let the row come back from a claimed span"
	);
	assert_eq!(after.len(), BUCKET_ROWS as usize - 1, "only the removed row may disappear");
}

struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		self.0 >> 11
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

fn run_interleaving(seed_value: u64) {
	// Randomised commit / remove / flush / scan interleavings against an in-test oracle of the latest write
	// per row. Every scan reads at the newest version, which is at or above every claim the read tier can
	// hold, so any divergence is a key the reader missed because a span was claimed without it. The oracle
	// is only ever consulted at the newest version because the sweep legitimately discards versions below
	// the eviction watermark.
	let (store, _g) = store();
	let mut rng = Rng(seed_value);
	let mut oracle: HashMap<Vec<u8>, (Vec<u8>, CommitVersion)> = HashMap::new();
	let mut version;
	let mut flushed_through = 0u64;

	seed(&store, 1);
	version = 1;
	for n in 1..=BUCKET_ROWS {
		oracle.insert(key(n * 2), (format!("v{n}").into_bytes(), CommitVersion(1)));
	}

	for step in 0..240 {
		match rng.below(10) {
			0..=4 => {
				version += 1;
				let row = 1 + rng.below(BUCKET_ROWS * 2 + 64);
				let value = format!("s{step}r{row}");
				commit_set(&store, row, version, &value);
				oracle.insert(key(row), (value.into_bytes(), CommitVersion(version)));
			}
			5..=6 => {
				version += 1;
				let row = 1 + rng.below(BUCKET_ROWS * 2 + 64);
				commit_remove(&store, row, version);
				oracle.remove(&key(row));
			}
			7..=8 => {
				flush(&store, version);
				flushed_through = version;
			}
			_ => {
				let _ = scan(&store, version);
			}
		}

		let seen = scan(&store, version);
		let expected: BTreeMap<Vec<u8>, (Vec<u8>, CommitVersion)> =
			oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		let seen_plain: BTreeMap<Vec<u8>, (Vec<u8>, CommitVersion)> =
			seen.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		assert_eq!(
			seen_plain,
			expected,
			"seed {seed_value} step {step}: a scan at version {version} diverged from the oracle \
			 (flushed through {flushed_through}, {} pages claimed complete)",
			complete_pages(&store)
		);
	}
}

#[test]
fn randomised_interleavings_never_miss_a_key_at_the_newest_version() {
	for seed_value in [1u64, 7, 42, 1337, 90210] {
		run_interleaving(seed_value);
	}
}

#[test]
fn concurrent_writers_flushes_and_installs_never_drop_a_key() {
	// A chunk install publishes a span from rows read outside every lock, then claims it. A key committed
	// and swept while that chunk is in flight is exactly the interleaving a version-stamped coverage
	// interval would have to defend against. Row number doubles as commit version, so a scan at the newest version
	// must contain every row the writer has already published, and any key the read-then-publish race drops shows
	// up as a gap.
	let (store, _g) = store();
	let rows: u64 = 1500;
	let published = Arc::new(std::sync::atomic::AtomicU64::new(0));

	std::thread::scope(|s| {
		let writer_store = store.clone();
		let writer_published = published.clone();
		s.spawn(move || {
			for row in 1..=rows {
				commit_set(&writer_store, row, row, &format!("r{row}"));
				writer_published.store(row, std::sync::atomic::Ordering::Release);
			}
		});

		let flusher_store = store.clone();
		let flusher_published = published.clone();
		s.spawn(move || {
			while flusher_published.load(std::sync::atomic::Ordering::Acquire) < rows {
				flusher_store.flush_all_blocking();
			}
			flusher_store.flush_all_blocking();
		});

		let reader_store = store.clone();
		let reader_published = published.clone();
		s.spawn(move || {
			loop {
				let known = reader_published.load(std::sync::atomic::Ordering::Acquire);
				let seen = scan(&reader_store, u64::MAX);
				for row in 1..=known {
					assert!(
						seen.contains_key(&key(row)),
						"row {row} was published at version {row} but a scan at the newest \
						 version did not return it; a claimed span dropped a key a concurrent \
						 writer or sweep had already made durable"
					);
				}
				if known >= rows {
					break;
				}
			}
		});
	});

	let final_scan = scan(&store, u64::MAX);
	assert_eq!(final_scan.len(), rows as usize, "every published row must survive the concurrent workload");
	assert!(installs(&store) > 0, "the workload must have installed at least one claim, or it proves nothing");
	assert!(complete_pages(&store) > 0, "the workload must have claimed at least one page, or it proves nothing");
}

#[test]
fn a_scan_below_the_persisted_high_water_installs_nothing() {
	// The scope gate stated as a property: a persistent chunk read at version R applies `version <= R`, so
	// every row written above R is invisible to it. A claim taken from such a chunk answers "absent" for
	// keys RAM never placed, and every later reader inherits that. Even rows are persisted at version 5 and
	// odd rows at version 50, so the scan at version 10 sees only half of them; the scan at 60 that follows
	// must still find all of them.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit_set(&store, n * 2, 5, &format!("even{n}"));
	}
	flush(&store, 5);
	for n in 1..=BUCKET_ROWS {
		commit_set(&store, n * 2 - 1, 50, &format!("odd{n}"));
	}
	flush(&store, 50);

	let stale = scan(&store, 10);
	assert_eq!(stale.len(), BUCKET_ROWS as usize, "only the rows written at version 5 are visible at 10");
	assert_eq!(installs(&store), 0, "a scan below the persisted high water must not install a claim");

	let fresh = scan(&store, 60);
	assert_eq!(
		fresh.len(),
		2 * BUCKET_ROWS as usize,
		"a claim taken from a chunk that could not see the newer rows reported those rows absent"
	);
	assert!(installs(&store) > 0, "the scan at version 60 must install, or the zero above proves nothing");
}

#[test]
fn a_windowed_scan_installs_nothing() {
	// A `Between` scope drops every key whose newest qualifying version is at or below the window's lower
	// end, so a chunk read under it speaks only for the versions inside the window. A claim taken from it
	// answers "absent" for the older rows it filtered out, and the `AsOf` scan that follows inherits that.
	let (store, _g) = store();
	for n in 1..=BUCKET_ROWS {
		commit_set(&store, n * 2, 5, &format!("even{n}"));
	}
	flush(&store, 5);
	for n in 1..=BUCKET_ROWS {
		commit_set(&store, n * 2 - 1, 50, &format!("odd{n}"));
	}
	flush(&store, 50);

	let windowed = scan_between(&store, 10, 60);
	assert_eq!(windowed.len(), BUCKET_ROWS as usize, "only the rows written above version 10 fall in the window");
	assert_eq!(installs(&store), 0, "a scan under a windowed scope must not install a claim");

	let full = scan(&store, 60);
	assert_eq!(
		full.len(),
		2 * BUCKET_ROWS as usize,
		"a claim taken from a windowed chunk reported the rows it filtered out absent"
	);
	assert!(installs(&store) > 0, "the unwindowed scan must install, or the zero above proves nothing");
}
