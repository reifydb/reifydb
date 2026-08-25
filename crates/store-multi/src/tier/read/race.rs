// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc, Barrier,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	thread,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::storage::StorageId, store::EntryKind},
	key::{
		EncodableKey, partitioned_row::PartitionedRowKey, partitioned_series_row::PartitionedSeriesRowKey,
		row::RowKey, series_row::SeriesRowKey,
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::{
	coverage::Edge,
	row::page::{PageId, page_of},
};
use reifydb_value::{
	byte_size::ByteSize,
	util::cowvec::CowVec,
	value::{partition::Partition, row_number::RowNumber},
};

use crate::tier::{
	RawEntry,
	read::{MultiReadBufferTier, ReadBufferConfig, ResidentPage, Span},
};

const SHIFT: u8 = 4;
const BUCKET: u64 = 1 << SHIFT;
const ROW_STORAGE: u64 = 7;
const PARTITIONED_STORAGE: u64 = 9;
const ROWS: u64 = 48;
const OTHERS: u64 = 4;

fn config(resident_pages: usize, resident_bytes: ByteSize) -> ReadBufferConfig {
	ReadBufferConfig {
		resident_pages,
		resident_bytes: Some(resident_bytes),
		shards: 1,
		bucket_shift: SHIFT,
	}
}

fn tier(resident_pages: usize, resident_bytes: ByteSize) -> MultiReadBufferTier {
	MultiReadBufferTier::new(config(resident_pages, resident_bytes))
		.expect("a tier with a byte budget must be constructed")
}

fn row(n: u64) -> EncodedKey {
	RowKey {
		storage: StorageId::table(ROW_STORAGE),
		row: RowNumber(n),
	}
	.encode()
}

fn series(n: u64) -> EncodedKey {
	SeriesRowKey {
		storage: StorageId::table(ROW_STORAGE),
		variant_tag: None,
		key: n,
		sequence: 0,
	}
	.encode()
}

fn partitioned(n: u64) -> EncodedKey {
	PartitionedRowKey::encoded(StorageId::table(PARTITIONED_STORAGE), Partition(n as u128), RowNumber(n))
}

fn partitioned_series(n: u64) -> EncodedKey {
	PartitionedSeriesRowKey::encoded(StorageId::table(PARTITIONED_STORAGE), Partition(n as u128), None, n, 0)
}

fn catalog(n: u64) -> EncodedKey {
	EncodedKey::new(format!("an-unclassifiable-catalog-key-{n}").into_bytes())
}

fn val(n: u64) -> CowVec<u8> {
	CowVec::new(n.to_be_bytes().to_vec())
}

fn page(key: &EncodedKey) -> PageId {
	page_of(key, SHIFT)
}

fn resident(read: &MultiReadBufferTier, key: &EncodedKey) -> bool {
	let id = page(key);
	read.shard_for(&id).lock().pages.get(&id).is_some_and(|page| page.entries.contains_key(key))
}

fn hull_of(read: &MultiReadBufferTier, id: PageId) -> Option<Span> {
	read.shard_for(&id).lock().pages.get(&id).and_then(|page| page.claimed.clone())
}

fn hot_page(read: &MultiReadBufferTier, id: PageId, tick: u64) {
	let mut shard = read.shard_for(&id).lock();
	let mut fresh = ResidentPage::fresh(tick);
	fresh.hot = true;
	shard.pages.insert(id, fresh);
}

/// Every key the model's persistent tier holds, across the five pages one pair of storages produces.
fn domain() -> Vec<EncodedKey> {
	let mut keys = Vec::new();
	for n in 0..ROWS {
		keys.push(row(n));
	}
	for n in 0..OTHERS {
		keys.push(series(n));
		keys.push(partitioned(n));
		keys.push(partitioned_series(n));
		keys.push(catalog(n));
	}
	keys
}

fn bucket_entries(bucket: u64, version: u64) -> Vec<RawEntry> {
	(0..ROWS)
		.filter(|n| n >> SHIFT == bucket)
		.map(|n| RawEntry {
			key: row(n),
			version: CommitVersion(version),
			value: Some(val(n)),
		})
		.collect()
}

struct Lcg(u64);

impl Lcg {
	fn next(&mut self) -> u64 {
		// A fixed generator pins each thread's operation sequence; the interleaving between threads is
		// what this test samples and is deliberately not pinned.
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		self.0 >> 33
	}
}

/// The first domain key a claim covers that RAM does not hold, or none.
///
/// The model's persistent tier holds every domain key at every instant, so a claim over a key that is
/// not resident is exactly the overstatement the coverage contract forbids. Understating is legal and
/// never reported. A claim over a key outside the domain is legal and is never inspected.
fn overstated(read: &MultiReadBufferTier, keys: &[EncodedKey]) -> Option<EncodedKey> {
	keys.iter().find(|key| read.covers(page(key).kind, key) && !resident(read, key)).cloned()
}

fn describe(read: &MultiReadBufferTier, key: &EncodedKey) -> String {
	let id = page(key);
	let present = {
		let shard = read.shard_for(&id).lock();
		shard.pages.get(&id).map(|page| page.entries.len())
	};
	format!("{:?} on {id:?}, page entries {present:?}", key.as_slice())
}

#[test]
fn an_invalidate_that_lands_inside_a_fill_window_must_not_leave_the_key_claimed() {
	// A fill places its rows, drops the shard lock, then publishes its claim under the coverage lock.
	// An invalidate of a key that is not yet claimed takes withdraw_key's early return, so it removes
	// the row from RAM without moving the retraction counter, and the fill's token still matches when
	// it publishes. The claim then stands over a key RAM does not hold and persistent still does.
	let fired = Arc::new(AtomicUsize::new(0));
	let read = {
		let fired = fired.clone();
		MultiReadBufferTier::with_interlock(
			config(8, ByteSize::from_gib(1)),
			Box::new(move |read, _page| {
				if fired.fetch_add(1, Ordering::Relaxed) != 1 {
					return;
				}
				read.invalidate(&row(1));
			}),
		)
		.expect("a tier with a byte budget must be constructed")
	};

	read.insert(row(0), CommitVersion(1), Some(val(1)));
	read.insert(row(1), CommitVersion(1), Some(val(1)));

	assert_eq!(fired.load(Ordering::Relaxed), 2, "the interlock must have run inside both fills");
	assert!(!resident(&read, &row(1)), "the interlock did not remove the key it invalidated");
	assert!(
		!read.covers(EntryKind::Source(StorageId::table(ROW_STORAGE)), &row(1)),
		"a fill republished a claim over a key the invalidate in its window removed from RAM"
	);
}

#[test]
fn a_drop_must_refuse_a_page_that_was_evicted_and_refilled_since_the_victim_was_chosen() {
	// Two evictors can hold the same victim at once. The first drops the page; a fill then recreates
	// it, and ResidentPage::fresh restarts the fill count at one. The second evictor's guard compares
	// a count that has come back round to the value it read, so it drops a page it never inspected and
	// the fill's claim survives the rows it was proving.
	let read = tier(2, ByteSize::from_gib(1));
	let kind = EntryKind::Source(StorageId::table(ROW_STORAGE));
	read.insert(row(0), CommitVersion(1), Some(val(1)));
	read.insert(row(BUCKET * 4), CommitVersion(1), Some(val(1)));
	hot_page(&read, page(&row(BUCKET * 8)), 9);
	let (victim, hull, fills) = read.pick_victim(0).expect("three pages against a cap of two");
	assert_eq!(victim, page(&row(0)), "the oldest hot page must be the victim");

	read.withdraw_span(victim.kind, &hull.expect("a fill records a hull"));
	read.invalidate(&row(0));
	assert!(!read.shard(0).lock().pages.contains_key(&victim), "the invalidate must retract the page");
	read.insert(row(1), CommitVersion(1), Some(val(1)));
	assert!(read.covers(kind, &row(1)), "the refill must claim its island");

	let dropped = read.drop_victim(0, victim, fills);

	assert!(
		!dropped || read.covers(kind, &row(1)) == resident(&read, &row(1)),
		"a stale drop removed a refilled page and left its claim standing over nothing"
	);
}

#[test]
fn a_row_page_hull_never_reaches_the_series_page_of_the_same_storage() {
	// Row and series keys of one storage share an EntryKind, so they share a CoverageSet, and the hull
	// retracts a byte span rather than a page id. If the two byte domains interleaved, evicting the row
	// page would silently withdraw claims over series rows still in RAM.
	let read = tier(8, ByteSize::from_gib(1));
	let kind = EntryKind::Source(StorageId::table(ROW_STORAGE));
	for n in 0..ROWS {
		read.insert(row(n), CommitVersion(1), Some(val(n)));
	}
	for n in 0..OTHERS {
		read.insert(series(n), CommitVersion(1), Some(val(n)));
	}
	assert_eq!(page(&row(0)).kind, kind);
	assert_eq!(page(&series(0)).kind, kind, "the two pages must share one coverage set");
	assert_ne!(page(&row(0)), page(&series(0)));

	for bucket in 0..ROWS.div_ceil(BUCKET) {
		let id = page(&row(bucket * BUCKET));
		let hull = hull_of(&read, id).expect("a fill records a hull");
		read.withdraw_span(id.kind, &hull);
	}

	for n in 0..ROWS {
		assert!(!read.covers(kind, &row(n)), "a row page hull did not retract its own page");
	}
	for n in 0..OTHERS {
		assert!(read.covers(kind, &series(n)), "a row page hull reached the series page of one storage");
	}
}

#[test]
fn a_partitioned_page_hull_never_reaches_the_partitioned_series_page_of_the_same_storage() {
	// Both kinds land on bucket zero of one PartitionedSource and neither has a reconstructable key
	// range, so their hulls are unions of single-key islands with nothing but the key encoding keeping
	// them apart.
	let read = tier(8, ByteSize::from_gib(1));
	let kind = EntryKind::PartitionedSource(StorageId::table(PARTITIONED_STORAGE));
	for n in 0..OTHERS {
		read.insert(partitioned(n), CommitVersion(1), Some(val(n)));
		read.insert(partitioned_series(n), CommitVersion(1), Some(val(n)));
	}
	assert_eq!(page(&partitioned(0)).kind, kind);
	assert_eq!(page(&partitioned_series(0)).kind, kind, "the two pages must share one coverage set");
	assert_ne!(page(&partitioned(0)), page(&partitioned_series(0)));

	let id = page(&partitioned(0));
	let hull = hull_of(&read, id).expect("a fill records a hull");
	read.withdraw_span(id.kind, &hull);

	for n in 0..OTHERS {
		assert!(!read.covers(kind, &partitioned(n)), "the hull did not retract its own page");
		assert!(
			read.covers(kind, &partitioned_series(n)),
			"a partitioned page hull reached the partitioned series page of one storage"
		);
	}
}

#[test]
fn a_hull_end_is_never_the_top_of_the_key_space() {
	// Edge::Top on a hull is the one shape that would let a retraction cross out of its own page's byte
	// domain no matter how the kinds are encoded.
	let read = tier(8, ByteSize::from_gib(1));
	for n in 0..OTHERS {
		read.insert(catalog(n), CommitVersion(1), Some(val(n)));
		read.insert(partitioned_series(n), CommitVersion(1), Some(val(n)));
	}

	for id in [page(&catalog(0)), page(&partitioned_series(0))] {
		let (_, end) = hull_of(&read, id).expect("a fill records a hull");
		assert_ne!(end, Edge::Top, "a hull that runs to the top retracts every kind above it");
	}
}

const THREADS: usize = 8;
const ROUNDS: usize = 300;
const STEPS: usize = 5;
const SEEDS: [u64; 8] = [1, 7, 13, 29, 101, 307, 911, 4517];

fn step(read: &MultiReadBufferTier, rng: &mut Lcg, keys: &[EncodedKey], version: &AtomicU64) {
	let key = keys[(rng.next() % keys.len() as u64) as usize].clone();
	let at = version.fetch_add(1, Ordering::SeqCst);
	match rng.next() % 100 {
		0..30 => read.insert(key, CommitVersion(at), Some(val(at))),
		30..40 => {
			let bucket = rng.next() % ROWS.div_ceil(BUCKET);
			read.populate_page(page(&row(bucket * BUCKET)), bucket_entries(bucket, at), true);
		}
		40..46 => {
			let bucket = rng.next() % ROWS.div_ceil(BUCKET);
			let id = page(&row(bucket * BUCKET));
			if read.begin_warm(id) {
				read.finish_warm(id, bucket_entries(bucket, at));
			}
		}
		46..70 => read.invalidate(&key),
		70..78 => read.remove_dropped(&key),
		78..84 => read.remove_dropped_through(&key, CommitVersion(at)),
		84..96 => read.evict_to_capacity(0),
		96..99 => {
			read.get(&key, CommitVersion(at));
		}
		_ => read.clear(),
	}
}

#[test]
fn concurrent_fills_evictions_and_invalidates_never_overstate_coverage() {
	// Coverage may understate what RAM holds and must never overstate it: an overstated claim makes a
	// future serve answer "nothing exists here" over keys the persistent tier still holds, silently.
	// The model's persistent tier holds every domain key always, so the check is that every claimed
	// domain key is resident.
	//
	// The check runs between two barriers with every thread parked, because coverage and the page map
	// are read under different locks; a check taken while a thread is mid-operation could tear across
	// them and report a violation that never existed. Once a violation is recorded the run continues,
	// so the anti-vacuity counters still describe the whole workload.
	let keys = domain();
	let mut evictions = 0u64;
	let mut published = 0u64;
	let mut refused = 0u64;
	let mut drops_refused = 0u64;
	let mut retractions = 0u64;
	let mut warms = 0u64;
	let mut dirty_aborts = 0u64;
	let violation: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

	for seed in SEEDS {
		let read = tier(3, ByteSize::from_bytes(8192));
		let barrier = Arc::new(Barrier::new(THREADS));
		let version = Arc::new(AtomicU64::new(1));
		let mut handles = Vec::with_capacity(THREADS);
		for id in 0..THREADS {
			let read = read.clone();
			let barrier = barrier.clone();
			let version = version.clone();
			let violation = violation.clone();
			let keys = keys.clone();
			handles.push(thread::spawn(move || {
				let mut rng = Lcg(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(id as u64));
				for round in 0..ROUNDS {
					for _ in 0..STEPS {
						step(&read, &mut rng, &keys, &version);
					}
					barrier.wait();
					if id == 0
						&& let Some(key) = overstated(&read, &keys)
					{
						let mut slot = violation.lock();
						if slot.is_none() {
							*slot = Some(format!(
								"seed {seed} round {round}: {}",
								describe(&read, &key)
							));
						}
					}
					barrier.wait();
				}
			}));
		}
		for handle in handles {
			handle.join().expect("a stress thread must not panic");
		}
		for shard in read.shard_metrics() {
			evictions += shard.warms.pages_evicted;
			warms += shard.warms.warms_completed;
			dirty_aborts += shard.warms.warms_dirty_aborted;
		}
		published += read.claims_published();
		refused += read.claims_refused();
		drops_refused += read.drops_refused();
		retractions += read.retractions();
	}

	assert_eq!(violation.lock().clone(), None, "coverage overstated what RAM holds");
	assert!(evictions > 1000, "only {evictions} evictions: the page cap never forced the retraction path");
	assert!(published > 1000, "only {published} claims published: nothing was ever claimed to overstate");
	assert!(retractions > 1000, "only {retractions} retractions: nothing was ever withdrawn");
	assert!(refused > 100, "only {refused} claims refused by a token: the fill-versus-shrink race is rare");
	assert!(drops_refused > 10, "only {drops_refused} drops refused: the fill-count guard never ran");
	assert!(warms > 100, "only {warms} warms completed: the warm claim path never ran");
	assert!(dirty_aborts > 0, "no warm was dirty-aborted, so a warm never raced a write");
	println!("COUNTERS evictions={evictions} published={published} retractions={retractions} refused={refused} drops_refused={drops_refused} warms={warms} dirty_aborts={dirty_aborts}");
}
