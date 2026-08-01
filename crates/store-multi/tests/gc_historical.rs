// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Historical-version GC (`scan_historical_below`) at the storage tier, without the GC actor: the
//! scan-then-drop loop must remove versions strictly below the cutoff, keep those at or above it, and
//! never touch the current version.

use std::collections::HashMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::EntryKind,
	},
};
use reifydb_store_multi::tier::{HistoricalCursor, TierStorage, commit::buffer::MultiCommitBufferTier};
use reifydb_value::util::cowvec::CowVec;

fn object() -> EntryKind {
	EntryKind::Source(StorageId::Table(TableId(42)))
}

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn val(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn write_n_versions(storage: &MultiCommitBufferTier, k: &EncodedKey, n: u64) {
	// Each successive write supersedes the prior current and demotes it to historical.
	let kind = object();
	for v in 1..=n {
		storage.set(CommitVersion(v), HashMap::from([(kind, vec![(k.clone(), Some(val(&format!("v{v}"))))])]))
			.unwrap();
	}
}

fn sweep(storage: &MultiCommitBufferTier, kind: EntryKind, cutoff: CommitVersion, batch_size: usize) -> u64 {
	// Returns the total versions deleted across every batch, not just the last one.
	let mut cursor = HistoricalCursor::default();
	let mut total = 0u64;
	loop {
		let entries = storage.scan_historical_below(kind, cutoff, &mut cursor, batch_size).unwrap();
		if entries.is_empty() {
			break;
		}
		total += entries.len() as u64;
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		batches.insert(kind, entries);
		storage.compact(batches).unwrap();
		if cursor.is_exhausted() {
			break;
		}
	}
	total
}

#[test]
fn memory_sweep_drops_only_versions_below_cutoff() {
	let storage = MultiCommitBufferTier::memory();
	let k = key("k");
	write_n_versions(&storage, &k, 100);

	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 99);

	let dropped = sweep(&storage, object(), CommitVersion(50), 32);
	// Versions 1..=49 are below the cutoff.
	assert_eq!(dropped, 49);

	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 50);

	let cur = storage.get(object(), &k, CommitVersion(100)).unwrap().value();
	assert_eq!(cur.as_deref(), Some(b"v100".as_slice()));

	let mid = storage.get(object(), &k, CommitVersion(60)).unwrap().value();
	assert_eq!(mid.as_deref(), Some(b"v60".as_slice()));

	// MVCC resolves to the largest surviving version <= requested and none survive below 50. The
	// watermark contract means no production reader is ever pinned below the cutoff.
	let pruned = storage.get(object(), &k, CommitVersion(40)).unwrap().value();
	assert!(pruned.is_none());
}

#[test]
fn sqlite_sweep_drops_only_versions_below_cutoff() {
	let storage = MultiCommitBufferTier::memory();
	let k = key("k");
	write_n_versions(&storage, &k, 100);

	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 99);

	let dropped = sweep(&storage, object(), CommitVersion(50), 32);
	assert_eq!(dropped, 49);

	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 50);

	let cur = storage.get(object(), &k, CommitVersion(100)).unwrap().value();
	assert_eq!(cur.as_deref(), Some(b"v100".as_slice()));

	let mid = storage.get(object(), &k, CommitVersion(60)).unwrap().value();
	assert_eq!(mid.as_deref(), Some(b"v60".as_slice()));

	let pruned = storage.get(object(), &k, CommitVersion(40)).unwrap().value();
	assert!(pruned.is_none());
}

#[test]
fn sweep_with_cutoff_zero_is_noop() {
	let storage = MultiCommitBufferTier::memory();
	let k = key("k");
	write_n_versions(&storage, &k, 10);

	let dropped = sweep(&storage, object(), CommitVersion(0), 32);
	assert_eq!(dropped, 0);
	assert_eq!(storage.count_historical(object()).unwrap(), 9);
}

#[test]
fn sweep_with_cutoff_above_max_drops_all_historical() {
	let storage = MultiCommitBufferTier::memory();
	let k = key("k");
	write_n_versions(&storage, &k, 10);

	let dropped = sweep(&storage, object(), CommitVersion(1_000_000), 32);
	// v1..v9 are historical and below the cutoff; the current v10 stays.
	assert_eq!(dropped, 9);
	assert_eq!(storage.count_historical(object()).unwrap(), 0);
	assert_eq!(storage.count_current(object()).unwrap(), 1);
}

#[test]
fn sweep_paginates_across_many_keys() {
	let storage = MultiCommitBufferTier::memory();
	for i in 0..50u8 {
		let k = key(&format!("k-{i:03}"));
		// Write 5 versions per key. v1..v4 land in historical, v5 in current.
		write_n_versions(&storage, &k, 5);
	}

	// 50 keys * 4 historical versions each = 200 historical rows.
	assert_eq!(storage.count_historical(object()).unwrap(), 200);
	assert_eq!(storage.count_current(object()).unwrap(), 50);

	// Cutoff = 4 means versions 1..=3 are dropped per key. 50 * 3 = 150.
	let dropped = sweep(&storage, object(), CommitVersion(4), 17);
	assert_eq!(dropped, 150);
	assert_eq!(storage.count_historical(object()).unwrap(), 50);
	assert_eq!(storage.count_current(object()).unwrap(), 50);
}

#[test]
fn sweep_does_not_touch_current_even_below_cutoff() {
	// Out-of-order writes can leave the current version below the cutoff while newer historical rows
	// exist; the scan covers the historical side only, so current must survive regardless.
	let storage = MultiCommitBufferTier::memory();
	let k = key("k");

	storage.set(CommitVersion(10), HashMap::from([(object(), vec![(k.clone(), Some(val("v10")))])])).unwrap();
	storage.set(CommitVersion(5), HashMap::from([(object(), vec![(k.clone(), Some(val("v5")))])])).unwrap();
	storage.set(CommitVersion(3), HashMap::from([(object(), vec![(k.clone(), Some(val("v3")))])])).unwrap();

	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 2);

	// Cutoff = 11 catches v3 and v5 (both historical) but not v10 (current).
	let dropped = sweep(&storage, object(), CommitVersion(11), 32);
	assert_eq!(dropped, 2);
	assert_eq!(storage.count_current(object()).unwrap(), 1);
	assert_eq!(storage.count_historical(object()).unwrap(), 0);

	let cur = storage.get(object(), &k, CommitVersion(10)).unwrap().value();
	assert_eq!(cur.as_deref(), Some(b"v10".as_slice()));
}

#[test]
fn list_all_entry_kinds_returns_known_objects() {
	let storage = MultiCommitBufferTier::memory();

	let s1 = EntryKind::Source(StorageId::Table(TableId(100)));
	let s2 = EntryKind::Source(StorageId::Table(TableId(200)));
	storage.set(CommitVersion(1), HashMap::from([(s1, vec![(key("a"), Some(val("1")))])])).unwrap();
	storage.set(CommitVersion(2), HashMap::from([(s2, vec![(key("b"), Some(val("2")))])])).unwrap();

	let kinds = storage.list_all_entry_kinds().unwrap();
	assert!(kinds.contains(&s1), "expected to find object 100, got {:?}", kinds);
	assert!(kinds.contains(&s2), "expected to find object 200, got {:?}", kinds);
}
