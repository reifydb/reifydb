// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Equivalence of the interval serve against the tier it substitutes for.
//!
//! The oracle is deliberately NOT a whole-page claim path. Such a path bails on the first page of every
//! scan that starts at a storage prefix, which is every scan in this codebase, so a differential test
//! against it would compare against a path that always answers "gap" and would pass whatever the serve
//! returned. The oracle here is a second store built with no read tier at all, driven by the same
//! commits, removals and flushes, so its scans reach the persistent tier and the commit buffer and
//! nothing else.
//!
//! Coverage may understate and must never overstate, so the failure this hunts is a scan that returns
//! fewer rows with the read tier than without it. Every test therefore carries an anti-vacuity assertion
//! on the count of chunks the interval path actually served: a run in which RAM answered nothing would
//! agree with the oracle for free.

use std::sync::Arc;

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
use reifydb_store_multi::{
	MultiVersionScope,
	store::{StandardMultiStore, multi::MultiVersionRangeCursor},
	tier::read::ReadBufferConfig,
};
use reifydb_value::{byte_size::ByteSize, cow_vec, util::cowvec::CowVec};

const STORAGES: [StorageId; 4] = [
	StorageId::Table(TableId(1)),
	StorageId::Table(TableId(2)),
	StorageId::Table(TableId(3)),
	StorageId::Table(TableId(4)),
];

/// Enough rows per storage that one scan spans several chunk materializes, so claims coalesce across chunks.
const ROWS: u64 = 160;

const BATCH: u64 = 64;

struct StaticWatermark(CommitVersion);

impl EvictionWatermark for StaticWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

/// Holds fewer pages than the workload touches, so a claim whose page has been evicted is exercised
/// rather than assumed away, and one shard so the page cap bites deterministically.
fn hot_config() -> ReadBufferConfig {
	ReadBufferConfig {
		resident_pages: 3,
		resident_bytes: Some(ByteSize::from_mib(4)),
		shards: 1,
		bucket_shift: 16,
	}
}

/// A read tier with no byte budget is never constructed, so this store's scans never consult one.
struct Pair {
	hot: StandardMultiStore,
	cold: StandardMultiStore,
	_guards: (Box<dyn std::any::Any>, Box<dyn std::any::Any>),
}

fn pair() -> Pair {
	pair_with(hot_config())
}

/// A pair whose read tier buckets at `hot.bucket_shift`, so a workload can be made to span several pages
/// of one storage rather than living entirely in the storage's last bucket.
fn pair_with(hot: ReadBufferConfig) -> Pair {
	let cold = ReadBufferConfig {
		resident_bytes: None,
		..hot
	};
	let (hot, hot_guard) = StandardMultiStore::testing_memory_with_persistent_sqlite_read(hot);
	let (cold, cold_guard) = StandardMultiStore::testing_memory_with_persistent_sqlite_read(cold);
	assert!(hot.read_buffer_shard_metrics().len() > 0, "the hot store must have a read tier");
	assert!(cold.read_buffer_shard_metrics().is_empty(), "the cold store must have no read tier to consult");
	Pair {
		hot,
		cold,
		_guards: (Box::new(hot_guard), Box::new(cold_guard)),
	}
}

fn commit_set(store: &StandardMultiStore, storage: StorageId, row: u64, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: RowKey::encoded(storage, row),
			bytes: EncodedBytes(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

fn commit_remove(store: &StandardMultiStore, storage: StorageId, row: u64, version: u64) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::remove_silent(RowKey::encoded(storage, row))],
		CommitVersion(version),
	)
	.unwrap();
}

fn flush(store: &StandardMultiStore, cutoff: u64) {
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(cutoff))));
	store.flush_pending_blocking();
}

type Row = (Vec<u8>, Vec<u8>, CommitVersion);

/// One batch's worth of rows plus the has-more flag, so a divergence in pagination is caught and not only
/// a divergence in content.
fn batches(store: &StandardMultiStore, storage: StorageId, read: u64) -> Vec<(Vec<Row>, bool)> {
	let mut cursor = MultiVersionRangeCursor::new();
	let mut out = Vec::new();
	loop {
		let batch = store
			.range_next(
				&mut cursor,
				RowKey::full_scan(storage),
				MultiVersionScope::AsOf {
					read: CommitVersion(read),
				},
				BATCH,
			)
			.unwrap();
		let rows: Vec<Row> =
			batch.items.iter().map(|r| (r.key.to_vec(), r.bytes.to_vec(), r.version)).collect();
		let more = batch.has_more;
		out.push((rows, more));
		if !more {
			return out;
		}
	}
}

fn rows_of(batches: &[(Vec<Row>, bool)]) -> Vec<Row> {
	batches.iter().flat_map(|(rows, _)| rows.iter().cloned()).collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct Served {
	chunks: u64,
	rows: u64,
	gaps: u64,
	refused: u64,
	materializes: u64,
	evicted: u64,
	head_advances: u64,
}

fn served(store: &StandardMultiStore) -> Served {
	let mut total = Served::default();
	for shard in store.read_buffer_shard_metrics() {
		total.chunks += shard.coverage.served;
		total.rows += shard.coverage.rows;
		total.gaps += shard.coverage.gaps;
		total.refused += shard.coverage.refused;
		total.materializes += shard.coverage.materializes;
		total.evicted += shard.pages.pages_evicted;
		total.head_advances += shard.coverage.head_advances;
	}
	total
}

fn assert_same(pair: &Pair, storage: StorageId, read: u64, context: &str) {
	let hot = batches(&pair.hot, storage, read);
	let cold = batches(&pair.cold, storage, read);
	assert_eq!(
		rows_of(&hot),
		rows_of(&cold),
		"{context}: a scan served from the interval coverage returned different rows than the same scan \
		 with no read tier at all; coverage claimed a span the persistent tier still had rows in"
	);
	assert!(
		!hot.last().expect("a scan always yields a batch").1
			&& !cold.last().expect("a scan always yields a batch").1,
		"{context}: a scan must end with has_more false on both stores, or the cursor never terminates"
	);
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

fn seed_both(pair: &Pair, version: u64) {
	for storage in STORAGES {
		for row in 1..=ROWS {
			let value = format!("seed-{}-{row}", storage_tag(storage));
			commit_set(&pair.hot, storage, row, version, &value);
			commit_set(&pair.cold, storage, row, version, &value);
		}
	}
}

fn storage_tag(storage: StorageId) -> u64 {
	match storage {
		StorageId::Table(TableId(id)) => id,
		_ => 0,
	}
}

fn run_equivalence(seed_value: u64) -> Served {
	run_equivalence_on(pair(), seed_value)
}

fn run_equivalence_on(pair: Pair, seed_value: u64) -> Served {
	let mut rng = Rng(seed_value);
	let mut version = 1u64;

	seed_both(&pair, version);
	flush(&pair.hot, version);
	flush(&pair.cold, version);

	for storage in STORAGES {
		let _ = batches(&pair.hot, storage, version);
		let _ = batches(&pair.hot, storage, version);
	}

	for step in 0..160 {
		let storage = STORAGES[rng.below(STORAGES.len() as u64) as usize];
		match rng.below(10) {
			0..=3 => {
				version += 1;
				let row = 1 + rng.below(ROWS + 32);
				let value = format!("s{step}r{row}");
				commit_set(&pair.hot, storage, row, version, &value);
				commit_set(&pair.cold, storage, row, version, &value);
			}
			4..=5 => {
				version += 1;
				let row = 1 + rng.below(ROWS + 32);
				commit_remove(&pair.hot, storage, row, version);
				commit_remove(&pair.cold, storage, row, version);
			}
			6..=7 => {
				flush(&pair.hot, version);
				flush(&pair.cold, version);
			}
			8 => {
				pair.hot.clear_read();
			}
			_ => {}
		}

		let scanned = STORAGES[rng.below(STORAGES.len() as u64) as usize];
		assert_same(&pair, scanned, version, &format!("seed {seed_value} step {step}"));
	}

	served(&pair.hot)
}

#[test]
fn interval_served_scans_match_a_store_with_no_read_tier() {
	// The highest-value test in the plan. Two stores take the identical commit, removal and flush stream;
	// one has a read tier and serves range chunks from its coverage claims, the other has none and can only
	// answer from the commit buffer and the persistent tier. Every scan must return the same rows in the
	// same batches. A serve that trusts a claim RAM no longer backs drops rows here and nowhere else.
	let mut total = Served::default();
	for seed_value in [1u64, 7, 42, 1337, 90210] {
		let seen = run_equivalence(seed_value);
		total.chunks += seen.chunks;
		total.rows += seen.rows;
		total.gaps += seen.gaps;
		total.refused += seen.refused;
		total.materializes += seen.materializes;
		total.evicted += seen.evicted;
		total.head_advances += seen.head_advances;
	}
	println!("coverage serve totals: {total:?}");

	assert!(
		total.chunks > 200,
		"the interval path must actually have served chunks, or the equivalence proves nothing: {total:?}"
	);
	assert!(
		total.rows > 2000,
		"the interval path must actually have carried rows out of RAM, or the equivalence proves \
		 nothing: {total:?}"
	);
	assert!(total.materializes > 20, "no materialize published a claim, so nothing was ever serveable: {total:?}");
	assert!(total.evicted > 0, "no page was evicted, so a claim whose page left RAM was never exercised");
	assert!(
		total.head_advances > 20,
		"no scan was moved off its storage prefix by a head, so the head path is untested here: {total:?}"
	);
}

/// Buckets small enough that one storage's rows span several pages, so the page holding a scan's first
/// rows is not the page its last rows are in.
fn paged_config() -> ReadBufferConfig {
	ReadBufferConfig {
		bucket_shift: 4,
		resident_pages: 24,
		..hot_config()
	}
}

#[test]
fn interval_served_scans_match_a_store_with_no_read_tier_across_several_pages() {
	// The same equivalence over a workload whose storages span ten pages rather than one. Every scan
	// starts at a storage prefix that names no page, so any serve of its leading chunk has to derive
	// the page rather than classify it; a derivation landing on the page a scan ends in, instead of the
	// one it starts in, drops every row of every page above. With one page per storage that mistake
	// returns the right rows anyway, so the single-page workload above cannot see it.
	let mut total = Served::default();
	for seed_value in [1u64, 7, 42, 1337, 90210] {
		let seen = run_equivalence_on(pair_with(paged_config()), seed_value);
		total.chunks += seen.chunks;
		total.rows += seen.rows;
		total.gaps += seen.gaps;
		total.refused += seen.refused;
		total.materializes += seen.materializes;
		total.evicted += seen.evicted;
		total.head_advances += seen.head_advances;
	}
	println!("paged coverage serve totals: {total:?}");

	assert!(
		total.chunks > 200,
		"the interval path must actually have served chunks, or the equivalence proves nothing: {total:?}"
	);
	assert!(
		total.rows > 2000,
		"the interval path must actually have carried rows out of RAM, or the equivalence proves \
		 nothing: {total:?}"
	);
	assert!(total.materializes > 20, "no materialize published a claim, so nothing was ever serveable: {total:?}");
	assert!(
		total.head_advances > 20,
		"no scan was moved off its storage prefix by a head, so the head path is untested here: {total:?}"
	);
}
