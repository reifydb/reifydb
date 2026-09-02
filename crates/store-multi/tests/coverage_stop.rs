// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Which stops a coverage claim may be taken from.
//!
//! `RangeCursor::exhausted` says a tier will yield nothing more. It does not say the tier read the range.
//! A tier that stopped because its readers are gone, or because it holds no table for the kind, read
//! nothing at all, and a claim stretched to the range end from such a chunk answers absent for every row
//! still beyond the resume point. Coverage may understate; it must never overstate.

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, EntryLayout, MultiVersionCommit},
	},
	key::row::RowKey,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_runtime::shutdown::Shutdown;
use reifydb_store_commit::{MultiVersionScope, RangeCursor, RangeStop};
use reifydb_store_multi::{
	store::{StandardMultiStore, multi::MultiVersionRangeCursor},
	tier::{TierStorage, persistent::MultiPersistentTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

/// The store's own persistent chunk size. Every boundary here is stated against it, so a chunk that comes
/// back exactly this full is the one case a short-page test must not confuse with a scan that ran out.
const CHUNK: u64 = 32;

const STORAGE: StorageId = StorageId::Table(TableId(1));

struct StaticWatermark(CommitVersion);

impl EvictionWatermark for StaticWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

fn store() -> (StandardMultiStore, impl Drop) {
	StandardMultiStore::testing_memory_with_persistent_sqlite()
}

fn commit_set(store: &StandardMultiStore, row: u64, version: u64) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: RowKey::encoded(STORAGE, row),
			bytes: EncodedBytes(CowVec::new(format!("v{row}").into_bytes())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

/// Drives the real flush engine so the rows leave the commit buffer and only the persistent tier still
/// answers for them; a scan that never reaches persistent proves nothing about what persistent claimed.
fn flush(store: &StandardMultiStore, cutoff: u64) {
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(cutoff))));
	store.flush_pending_blocking();
}

fn materializes(store: &StandardMultiStore) -> u64 {
	store.range_shard_metrics().iter().map(|s| s.counters.materializes).sum()
}

fn chunk(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor, read: u64) -> (usize, bool) {
	let batch = store
		.range_next(
			cursor,
			RowKey::full_scan(STORAGE),
			MultiVersionScope::AsOf {
				read: CommitVersion(read),
			},
			CHUNK,
		)
		.unwrap();
	(batch.items.len(), batch.has_more)
}

fn tier_with_rows(rows: u64, version: u64) -> (MultiPersistentTier, impl Drop) {
	let (tier, guard) = MultiPersistentTier::sqlite_in_memory();
	if rows > 0 {
		let mut batch = HashMap::new();
		batch.insert(
			EntryKind::Source(STORAGE, EntryLayout::Row),
			(1..=rows)
				.map(|row| {
					(
						RowKey::encoded(STORAGE, row),
						Some(CowVec::new(format!("v{row}").into_bytes())),
					)
				})
				.collect::<Vec<_>>(),
		);
		tier.set(CommitVersion(version), batch).unwrap();
	}
	(tier, guard)
}

fn tier_chunk(tier: &MultiPersistentTier, cursor: &mut RangeCursor, read: u64) -> usize {
	tier.range_next(
		EntryKind::Source(STORAGE, EntryLayout::Row),
		cursor,
		Bound::Unbounded,
		Bound::Unbounded,
		MultiVersionScope::AsOf {
			read: CommitVersion(read),
		},
		CHUNK as usize,
	)
	.unwrap()
	.entries
	.len()
}

#[test]
fn a_scan_stopped_by_a_drained_reader_pool_materializes_no_claim() {
	// The defect: an empty chunk still stretched its claim to the range end; the read now refuses outright instead.
	let (store, _g) = store();
	for row in 1..=(CHUNK * 6) {
		commit_set(&store, row, 10);
	}
	flush(&store, 20);

	let mut cursor = MultiVersionRangeCursor::new();
	let (first, more) = chunk(&store, &mut cursor, 30);
	assert_eq!(first as u64, CHUNK, "the first chunk must fill, or the scan never resumes past it");
	assert!(more, "rows must remain, or the shutdown below lands on an already finished scan");
	let materialized = materializes(&store);
	assert!(materialized > 0, "the first chunk must materialize its own span, or the delta below proves nothing");

	Shutdown::shutdown(&store);

	let refused = store.range_next(
		&mut cursor,
		RowKey::full_scan(STORAGE),
		MultiVersionScope::AsOf {
			read: CommitVersion(30),
		},
		CHUNK,
	);
	assert!(refused.is_err(), "a drained pool must refuse the read, not answer it as a finished range");
	assert_eq!(
		materializes(&store),
		materialized,
		"a chunk that read nothing because the tier is shut down claimed the rest of the range anyway; \
		 every row past the resume point is now absent as far as RAM is concerned"
	);
}

#[test]
fn a_scan_that_read_to_the_range_end_materializes_a_claim() {
	// The partner of the test above: the only stop a claim may be taken from must still take it, or the
	// fix would have been a blanket refusal and the read tier would never serve a range again.
	let (store, _g) = store();
	for row in 1..=(CHUNK * 2 + 5) {
		commit_set(&store, row, 10);
	}
	flush(&store, 20);

	let mut cursor = MultiVersionRangeCursor::new();
	let mut total = 0;
	loop {
		let (rows, more) = chunk(&store, &mut cursor, 30);
		total += rows as u64;
		if !more {
			break;
		}
	}
	assert_eq!(total, CHUNK * 2 + 5, "the scan must return every flushed row");
	assert!(materializes(&store) > 0, "a scan that read the range to its end must claim what it read");
}

#[test]
fn a_chunk_over_an_absent_table_is_not_a_scan_to_the_range_end() {
	// Naming an unflushed keyspace's absent table a scan to the end would hand the materialize path a span no read
	// ever examined.
	let (tier, _g) = tier_with_rows(0, 5);
	let mut cursor = RangeCursor::new();
	let rows = tier_chunk(&tier, &mut cursor, 30);

	assert_eq!(rows, 0, "an absent table yields no rows");
	assert!(cursor.is_exhausted(), "an absent table must still end the scan, or the store's loop spins forever");
	assert_eq!(cursor.stop(), Some(&RangeStop::AbsentTable), "the stop must name the absent table");
	assert!(!cursor.scanned_to_end(), "no read happened, so no claim may be taken from this stop");
}

#[test]
fn a_chunk_that_read_a_present_table_to_its_end_is_a_scan_to_the_range_end() {
	// The partner of the absent-table test: a short page is the one stop the materialize path may stretch to the
	// range end.
	let (tier, _g) = tier_with_rows(CHUNK / 2, 5);
	let mut cursor = RangeCursor::new();
	let rows = tier_chunk(&tier, &mut cursor, 30);

	assert_eq!(rows as u64, CHUNK / 2, "the whole table fits in one chunk");
	assert!(cursor.is_exhausted());
	assert_eq!(cursor.stop(), Some(&RangeStop::Scanned), "a short page is a read that reached the range end");
	assert!(cursor.scanned_to_end(), "this is the stop a claim is taken from");
}

#[test]
fn a_chunk_stopped_by_a_drained_reader_pool_is_not_a_scan_to_the_range_end() {
	// Read at the tier so the classification is pinned even if the materialize path above is rewritten.
	let (tier, _g) = tier_with_rows(CHUNK * 2, 5);
	let mut cursor = RangeCursor::new();
	assert_eq!(tier_chunk(&tier, &mut cursor, 30) as u64, CHUNK, "the first chunk must fill");
	assert!(cursor.last_key().is_some(), "the cursor must have resumed on a row key");

	tier.shutdown();

	let refused = tier.range_next(
		EntryKind::Source(STORAGE, EntryLayout::Row),
		&mut cursor,
		Bound::Unbounded,
		Bound::Unbounded,
		MultiVersionScope::AsOf {
			read: CommitVersion(30),
		},
		CHUNK as usize,
	);
	assert!(refused.is_err(), "a drained pool must refuse the read, not answer it as a finished range");
	assert!(!cursor.scanned_to_end(), "no read happened, so no claim may be taken from this stop");
}

#[test]
fn a_chunk_that_comes_back_exactly_full_is_not_the_end_of_the_range() {
	// The boundary that decides "genuinely over" is whether the page came back full. A table holding
	// exactly two chunks makes the first chunk exactly full with a whole chunk still behind it, so an
	// off-by-one that reads a full page as short drops every row past the first chunk.
	let (tier, _g) = tier_with_rows(CHUNK * 2, 5);
	let mut cursor = RangeCursor::new();

	assert_eq!(tier_chunk(&tier, &mut cursor, 30) as u64, CHUNK);
	assert!(!cursor.is_exhausted(), "a full page proves nothing beyond itself; a whole chunk still follows it");
	assert_eq!(cursor.stop(), None, "a chunk that did not stop must leave no stop reason behind");

	assert_eq!(tier_chunk(&tier, &mut cursor, 30) as u64, CHUNK);
	assert!(!cursor.is_exhausted(), "the second page is full too, so the range is still not proven over");

	assert_eq!(tier_chunk(&tier, &mut cursor, 30), 0, "the table holds exactly two chunks");
	assert!(cursor.is_exhausted());
	assert_eq!(cursor.stop(), Some(&RangeStop::Scanned));
}

#[test]
fn a_scan_of_an_exact_multiple_of_the_chunk_size_returns_every_row() {
	// The store-level shape of the boundary above: a row count that is an exact multiple of the chunk size
	// is where a full page misread as short truncates the scan silently, with has_more false and no error.
	let (store, _g) = store();
	for row in 1..=(CHUNK * 3) {
		commit_set(&store, row, 10);
	}
	flush(&store, 20);

	let mut cursor = MultiVersionRangeCursor::new();
	let mut total = 0;
	loop {
		let (rows, more) = chunk(&store, &mut cursor, 30);
		total += rows as u64;
		if !more {
			break;
		}
	}
	assert_eq!(total, CHUNK * 3, "a scan whose row count divides the chunk size exactly must lose nothing");
}
