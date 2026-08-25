// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a read of the persistent tier answers once its reader pool is drained.
//!
//! Shutting the tier down is expected. Answering as though the range were read is not: a chunk that
//! reports itself exhausted having opened no connection turns a partial scan into a complete one, and the
//! caller has no way to tell that apart from a range that genuinely ended. The write path already refuses
//! rather than acknowledge a flush against a closed store; these are the matching refusals on the read
//! side.

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionCommit},
	},
	key::row::RowKey,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_runtime::shutdown::Shutdown;
use reifydb_store_multi::{
	MultiVersionScope,
	store::{StandardMultiStore, multi::MultiVersionRangeCursor},
	tier::{RangeCursor, TierStorage, persistent::MultiPersistentTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

/// The store's own persistent chunk size, so a row count stated as a multiple of it lands a known number
/// of chunks either side of the shutdown.
const CHUNK: u64 = 32;

const STORAGE: StorageId = StorageId::Table(TableId(1));

const WRITTEN_AT: u64 = 5;

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

/// Drives the real flush engine so the rows leave the commit buffer; without it a scan never reaches
/// persistent and so never reaches the drained pool either.
fn flush(store: &StandardMultiStore, cutoff: u64) {
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(cutoff))));
	store.flush_pending_blocking();
}

fn as_of(read: u64) -> MultiVersionScope {
	MultiVersionScope::AsOf {
		read: CommitVersion(read),
	}
}

fn tier_with_rows(rows: u64) -> (MultiPersistentTier, impl Drop) {
	let (tier, guard) = MultiPersistentTier::sqlite_in_memory();
	let mut batch = HashMap::new();
	batch.insert(
		EntryKind::Source(STORAGE),
		(1..=rows)
			.map(|row| (RowKey::encoded(STORAGE, row), Some(CowVec::new(format!("v{row}").into_bytes()))))
			.collect::<Vec<_>>(),
	);
	tier.set(CommitVersion(WRITTEN_AT), batch).unwrap();
	(tier, guard)
}

fn tier_chunk(tier: &MultiPersistentTier, cursor: &mut RangeCursor) -> reifydb_value::Result<usize> {
	Ok(tier.range_next(
		EntryKind::Source(STORAGE),
		cursor,
		Bound::Unbounded,
		Bound::Unbounded,
		as_of(30),
		CHUNK as usize,
	)?
	.entries
	.len())
}

fn consistent_range(tier: &MultiPersistentTier) -> reifydb_value::Result<usize> {
	Ok(tier.load_range_consistent(
		EntryKind::Source(STORAGE),
		Bound::Unbounded,
		Bound::Unbounded,
		CommitVersion(30),
		None,
	)?
	.len())
}

/// A refusal must be debuggable from its message alone, so it names the condition and not just the failure.
fn names_the_shutdown(error: &reifydb_value::error::Error) {
	let message = error.to_string();
	assert!(message.contains("shut down"), "the error must name the shut-down store; got {message}");
}

#[test]
fn a_range_chunk_on_a_drained_reader_pool_fails_instead_of_reporting_the_range_over() {
	// A drained pool that sets exhausted turns a scan that read one chunk of two into the whole range.
	let (tier, _g) = tier_with_rows(CHUNK * 2);
	let mut cursor = RangeCursor::new();
	assert_eq!(tier_chunk(&tier, &mut cursor).unwrap() as u64, CHUNK, "the first chunk must fill");
	// An already exhausted cursor short-circuits, so without this the refusal below would be vacuous.
	assert!(!cursor.exhausted, "a full page must leave rows behind it");

	tier.shutdown();

	let error = tier_chunk(&tier, &mut cursor).expect_err(
		"a chunk that opened no connection reported the range over; every row past the resume point is \
		 silently dropped",
	);
	names_the_shutdown(&error);
}

#[test]
fn a_range_chunk_on_a_live_tier_still_reads_to_the_range_end() {
	// A blanket refusal would satisfy the test above while taking the tier permanently out of service.
	let (tier, _g) = tier_with_rows(CHUNK / 2);
	let mut cursor = RangeCursor::new();

	assert_eq!(tier_chunk(&tier, &mut cursor).unwrap() as u64, CHUNK / 2, "a live tier serves its rows");
	assert!(cursor.scanned_to_end(), "a short page on a live tier is still a read that reached the end");
}

#[test]
fn a_store_scan_resumed_across_a_shutdown_fails_instead_of_returning_a_short_result() {
	// The refusal must survive the store's buffer-then-persistent step loop, not stop at the tier.
	let (store, _g) = store();
	for row in 1..=(CHUNK * 6) {
		commit_set(&store, row, 10);
	}
	flush(&store, 20);

	let mut cursor = MultiVersionRangeCursor::new();
	let first = store.range_next(&mut cursor, RowKey::full_scan(STORAGE), as_of(30), CHUNK).unwrap();
	assert_eq!(first.items.len() as u64, CHUNK, "the first chunk must fill");
	// Five chunks must remain, or the shutdown lands on an already finished scan and proves nothing.
	assert!(first.has_more, "rows must remain behind the resume point");

	Shutdown::shutdown(&store);

	let error = store
		.range_next(&mut cursor, RowKey::full_scan(STORAGE), as_of(30), CHUNK)
		.expect_err("the scan returned a short result and called it complete");
	names_the_shutdown(&error);
}

#[test]
fn an_iteration_that_straddles_a_shutdown_yields_an_error_rather_than_ending() {
	// A None here reads as the end of the data, so every row past it is silently deleted downstream.
	let (store, _g) = store();
	for row in 1..=(CHUNK * 6) {
		commit_set(&store, row, 10);
	}
	flush(&store, 20);

	let mut rows = store.range(RowKey::full_scan(STORAGE), as_of(30), CHUNK as usize);
	for _ in 0..CHUNK {
		rows.next().expect("the first chunk must yield rows").expect("a live scan must not fail");
	}

	Shutdown::shutdown(&store);

	match rows.next() {
		None => panic!(
			"the iteration ended at {CHUNK} of {} rows and reported that as the end of the range",
			CHUNK * 6
		),
		Some(Ok(row)) => panic!("a drained pool served row {:?}", row.key),
		Some(Err(error)) => names_the_shutdown(&error),
	}
}

#[test]
fn a_consistent_range_on_a_drained_reader_pool_fails_instead_of_reporting_no_rows() {
	// An empty vector is the same sentence as "these keys do not exist" and the caller cannot tell which.
	let (tier, _g) = tier_with_rows(CHUNK);
	// Asserted live first, so the refusal below is known to come from the drain and not an empty table.
	assert_eq!(consistent_range(&tier).unwrap() as u64, CHUNK, "a live tier answers with the rows it holds");

	tier.shutdown();

	let error = consistent_range(&tier)
		.expect_err("a drained pool answered no rows, which reads as the rows not existing");
	names_the_shutdown(&error);
}

#[test]
fn the_install_floor_of_a_drained_reader_pool_fails_instead_of_answering_the_max_version() {
	// The u64::MAX fallback is memoized, so a first probe after the drain pins the floor forever.
	let (tier, _g) = tier_with_rows(CHUNK);
	tier.shutdown();

	let error = tier.install_floor().expect_err("a floor probed against no connection answered a version");
	names_the_shutdown(&error);
}

#[test]
fn the_install_floor_of_a_live_tier_answers_the_highest_version_on_disk() {
	// Erroring unconditionally would stop the read tier ever claiming a span again.
	let (tier, _g) = tier_with_rows(CHUNK);

	assert_eq!(
		tier.install_floor().unwrap(),
		CommitVersion(WRITTEN_AT),
		"a live tier must report the highest version it holds"
	);
}
