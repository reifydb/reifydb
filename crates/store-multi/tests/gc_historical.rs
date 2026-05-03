// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Integration tests for the historical-version GC primitive (`scan_historical_below`).
//!
//! These exercise the storage-tier piece of the GC actor without spinning up
//! the actor itself - we verify that the scan-then-drop loop correctly removes
//! versions strictly below a given cutoff, leaves versions at or above the
//! cutoff intact, and never touches `__current`.

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
};
use reifydb_store_multi::{
	buffer::storage::BufferStorage,
	tier::{HistoricalCursor, TierStorage},
};
use reifydb_type::util::cowvec::CowVec;

fn shape() -> EntryKind {
	EntryKind::Source(ShapeId::Table(TableId(42)))
}

fn key(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn val(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

/// Write `n` versions of the same key to the same shape. Each successive write
/// supersedes the prior current and demotes it to historical.
fn write_n_versions(storage: &BufferStorage, k: &CowVec<u8>, n: u64) {
	let kind = shape();
	for v in 1..=n {
		storage.set(CommitVersion(v), HashMap::from([(kind, vec![(k.clone(), Some(val(&format!("v{v}"))))])]))
			.unwrap();
	}
}

/// Drain `scan_historical_below` into `drop` until the cursor is exhausted.
/// Returns the total number of versions deleted across all batches.
fn sweep(storage: &BufferStorage, kind: EntryKind, cutoff: CommitVersion, batch_size: usize) -> u64 {
	let mut cursor = HistoricalCursor::default();
	let mut total = 0u64;
	loop {
		let entries = storage.scan_historical_below(kind, cutoff, &mut cursor, batch_size).unwrap();
		if entries.is_empty() {
			break;
		}
		total += entries.len() as u64;
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();
		batches.insert(kind, entries);
		storage.drop(batches).unwrap();
		if cursor.is_exhausted() {
			break;
		}
	}
	total
}

#[test]
fn memory_sweep_drops_only_versions_below_cutoff() {
	let storage = BufferStorage::memory();
	let k = key("k");
	write_n_versions(&storage, &k, 100);

	// __current = v100. __historical = v1..v99.
	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 99);

	let dropped = sweep(&storage, shape(), CommitVersion(50), 32);
	// Versions 1..=49 are below cutoff (49 versions).
	assert_eq!(dropped, 49);

	// Current untouched.
	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 50);

	// Reading at the latest snapshot still returns v100.
	let cur = storage.get(shape(), &k, CommitVersion(100)).unwrap();
	assert_eq!(cur.as_deref(), Some(b"v100".as_slice()));

	// Reading at v60 still resolves to v60 (above cutoff, retained).
	let mid = storage.get(shape(), &k, CommitVersion(60)).unwrap();
	assert_eq!(mid.as_deref(), Some(b"v60".as_slice()));

	// Reading at v40 returns None: v40 was pruned and standard MVCC `get`
	// resolves to the largest version <= requested, of which none survive.
	// In production this query never happens - the watermark contract says
	// no reader is below cutoff.
	let pruned = storage.get(shape(), &k, CommitVersion(40)).unwrap();
	assert!(pruned.is_none());
}

#[test]
fn sqlite_sweep_drops_only_versions_below_cutoff() {
	let storage = BufferStorage::sqlite_in_memory();
	let k = key("k");
	write_n_versions(&storage, &k, 100);

	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 99);

	let dropped = sweep(&storage, shape(), CommitVersion(50), 32);
	assert_eq!(dropped, 49);

	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 50);

	let cur = storage.get(shape(), &k, CommitVersion(100)).unwrap();
	assert_eq!(cur.as_deref(), Some(b"v100".as_slice()));

	let mid = storage.get(shape(), &k, CommitVersion(60)).unwrap();
	assert_eq!(mid.as_deref(), Some(b"v60".as_slice()));

	let pruned = storage.get(shape(), &k, CommitVersion(40)).unwrap();
	assert!(pruned.is_none());
}

#[test]
fn sweep_with_cutoff_zero_is_noop() {
	let storage = BufferStorage::sqlite_in_memory();
	let k = key("k");
	write_n_versions(&storage, &k, 10);

	let dropped = sweep(&storage, shape(), CommitVersion(0), 32);
	assert_eq!(dropped, 0);
	assert_eq!(storage.count_historical(shape()).unwrap(), 9);
}

#[test]
fn sweep_with_cutoff_above_max_drops_all_historical() {
	let storage = BufferStorage::sqlite_in_memory();
	let k = key("k");
	write_n_versions(&storage, &k, 10);

	let dropped = sweep(&storage, shape(), CommitVersion(1_000_000), 32);
	// All 9 historical versions (v1..v9) are below cutoff. Current v10 stays.
	assert_eq!(dropped, 9);
	assert_eq!(storage.count_historical(shape()).unwrap(), 0);
	assert_eq!(storage.count_current(shape()).unwrap(), 1);
}

#[test]
fn sweep_paginates_across_many_keys() {
	let storage = BufferStorage::sqlite_in_memory();
	for i in 0..50u8 {
		let k = key(&format!("k-{i:03}"));
		// Write 5 versions per key. v1..v4 land in historical, v5 in current.
		write_n_versions(&storage, &k, 5);
	}

	// 50 keys * 4 historical versions each = 200 historical rows.
	assert_eq!(storage.count_historical(shape()).unwrap(), 200);
	assert_eq!(storage.count_current(shape()).unwrap(), 50);

	// Cutoff = 4 means versions 1..=3 are dropped per key. 50 * 3 = 150.
	let dropped = sweep(&storage, shape(), CommitVersion(4), 17);
	assert_eq!(dropped, 150);
	assert_eq!(storage.count_historical(shape()).unwrap(), 50);
	assert_eq!(storage.count_current(shape()).unwrap(), 50);
}

#[test]
fn sweep_does_not_touch_current_even_below_cutoff() {
	// Edge: if writes were out of order so current.version < cutoff but a
	// newer historical exists, we still must not delete from current. The
	// scan is purely over the __historical table.
	let storage = BufferStorage::sqlite_in_memory();
	let k = key("k");

	// Write v10 first (lands in current).
	storage.set(CommitVersion(10), HashMap::from([(shape(), vec![(k.clone(), Some(val("v10")))])])).unwrap();
	// Write v5 second (out-of-order; lands in historical).
	storage.set(CommitVersion(5), HashMap::from([(shape(), vec![(k.clone(), Some(val("v5")))])])).unwrap();
	// Write v3 (also historical).
	storage.set(CommitVersion(3), HashMap::from([(shape(), vec![(k.clone(), Some(val("v3")))])])).unwrap();

	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 2);

	// Cutoff = 11 catches v3 and v5 (both historical) but not v10 (current).
	let dropped = sweep(&storage, shape(), CommitVersion(11), 32);
	assert_eq!(dropped, 2);
	assert_eq!(storage.count_current(shape()).unwrap(), 1);
	assert_eq!(storage.count_historical(shape()).unwrap(), 0);

	let cur = storage.get(shape(), &k, CommitVersion(10)).unwrap();
	assert_eq!(cur.as_deref(), Some(b"v10".as_slice()));
}

#[test]
fn list_all_entry_kinds_returns_known_shapes() {
	let storage = BufferStorage::sqlite_in_memory();

	let s1 = EntryKind::Source(ShapeId::Table(TableId(100)));
	let s2 = EntryKind::Source(ShapeId::Table(TableId(200)));
	storage.set(CommitVersion(1), HashMap::from([(s1, vec![(key("a"), Some(val("1")))])])).unwrap();
	storage.set(CommitVersion(2), HashMap::from([(s2, vec![(key("b"), Some(val("2")))])])).unwrap();

	let kinds = storage.list_all_entry_kinds().unwrap();
	// Each insert touches both `__current` and `__historical` of that shape.
	// We expect both source IDs to be enumerated.
	assert!(kinds.contains(&s1), "expected to find shape 100, got {:?}", kinds);
	assert!(kinds.contains(&s2), "expected to find shape 200, got {:?}", kinds);
}
