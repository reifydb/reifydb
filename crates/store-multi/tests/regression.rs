// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionCommit},
	},
	key::row::RowKey,
};
use reifydb_store_multi::{
	MultiVersionScope,
	store::StandardMultiStore,
	tier::{RangeBatch, RangeCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec};

fn k(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn v(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn object() -> EntryKind {
	EntryKind::Source(StorageId::Table(TableId(2024)))
}

fn drain_forward(
	s: &MultiCommitBufferTier,
	kind: EntryKind,
	version: CommitVersion,
	batch_size: usize,
) -> Vec<Vec<u8>> {
	let mut cursor = RangeCursor::new();
	let mut out = Vec::new();
	loop {
		let RangeBatch {
			entries,
			has_more,
		} = s.range_next(
			kind,
			&mut cursor,
			Bound::Unbounded,
			Bound::Unbounded,
			MultiVersionScope::AsOf {
				read: version,
			},
			batch_size,
		)
		.unwrap();
		for e in entries {
			out.push(e.key.as_slice().to_vec());
		}
		if !has_more || cursor.is_exhausted() {
			break;
		}
	}
	out
}

#[test]
fn paginated_range_does_not_truncate_when_filtered_key_is_inside_limit_window() {
	for storage in [MultiCommitBufferTier::memory()] {
		let kind = object();

		for key in ["a", "b", "d", "e", "f", "g", "h", "i"] {
			storage.set(CommitVersion(1), HashMap::from([(kind, vec![(k(key), Some(v("v1")))])])).unwrap();
		}

		storage.set(CommitVersion(10), HashMap::from([(kind, vec![(k("c"), Some(v("v10")))])])).unwrap();

		let result = drain_forward(&storage, kind, CommitVersion(3), 7);

		let recovered: Vec<&str> = result.iter().map(|kk| std::str::from_utf8(kk).unwrap()).collect();
		assert_eq!(
			recovered,
			vec!["a", "b", "d", "e", "f", "g", "h", "i"],
			"expected 8 keys; c is in __current at v=10 with no v<=3 history (filtered out at this snapshot), but the range scan must still page past it to reach h and i past the SQL LIMIT window"
		);
	}
}

#[test]
fn paginated_range_includes_trailing_tombstone_after_filter_skip() {
	for storage in [MultiCommitBufferTier::memory()] {
		let kind = object();

		for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
			storage.set(CommitVersion(1), HashMap::from([(kind, vec![(k(key), Some(v("init")))])]))
				.unwrap();
		}

		storage.set(CommitVersion(2), HashMap::from([(kind, vec![(k("z"), None)])])).unwrap();

		storage.set(CommitVersion(8), HashMap::from([(kind, vec![(k("a"), Some(v("v8")))])])).unwrap();

		let result = drain_forward(&storage, kind, CommitVersion(3), 7);

		let z_visible = result.iter().any(|kk| kk == b"z");
		assert!(z_visible, "tombstone z at v=2 must appear in v=3 paginated range; got {:?}", result);
	}
}

const STORAGE: StorageId = StorageId::Table(TableId(1));

const SCAN_CHUNK: u64 = 32;

const STALE_SCOPE: MultiVersionScope = MultiVersionScope::Between {
	after: CommitVersion(50),
	read: CommitVersion(60),
};

/// Two full persistent chunks of v1 rows plus one v55 row in the commit buffer.
fn store_with_two_stale_chunks(stale_from: u64, fresh_row: u64) -> (StandardMultiStore, impl Drop) {
	// Exactly two FULL chunks: a short chunk exhausts the sqlite cursor and hides the defect.
	let (store, guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let persistent = store.persistent().expect("persistent tier configured");

	let stale: Vec<(EncodedKey, Option<CowVec<u8>>)> = (stale_from..(stale_from + 2 * SCAN_CHUNK))
		.map(|row| (RowKey::encoded(STORAGE, row), Some(v("stale"))))
		.collect();
	persistent.set(CommitVersion(1), HashMap::from([(EntryKind::Source(STORAGE), stale)])).unwrap();

	MultiVersionCommit::commit(
		&store,
		cow_vec![Delta::Set {
			key: RowKey::encoded(STORAGE, fresh_row),
			bytes: EncodedBytes(v("fresh")),
		}],
		CommitVersion(55),
	)
	.unwrap();

	(store, guard)
}

#[test]
fn between_scan_keeps_commit_rows_above_a_fully_filtered_persistent_chunk() {
	// Row 1 holds the highest key, so a forward trim to a still-advancing persistent cursor drops it.
	let (store, _guard) = store_with_two_stale_chunks(2, 1);

	let scanned: Vec<(Vec<u8>, u64)> = store
		.range(RowKey::full_scan(STORAGE), STALE_SCOPE, 10)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|r| (r.bytes.to_vec(), r.version.0))
		.collect();

	assert_eq!(
		scanned,
		vec![(b"fresh".to_vec(), 55)],
		"the v55 row must survive: every persistent row is v1 and fails Between's exclusive lower bound, \
		 so no surviving row is evidence that sqlite has run out of rows, and trimming the collected set \
		 down to a still-advancing persistent cursor drops a row that no later batch will re-emit"
	);
}

#[test]
fn between_rev_scan_keeps_commit_rows_below_a_fully_filtered_persistent_chunk() {
	// The reverse horizon retains keys at or above itself, so the row at risk is the lowest key.
	let (store, _guard) = store_with_two_stale_chunks(1, 2 * SCAN_CHUNK + 1);

	let scanned: Vec<(Vec<u8>, u64)> = store
		.range_rev(RowKey::full_scan(STORAGE), STALE_SCOPE, 10)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|r| (r.bytes.to_vec(), r.version.0))
		.collect();

	assert_eq!(
		scanned,
		vec![(b"fresh".to_vec(), 55)],
		"the v55 row must survive a descending scan for the same reason as the forward one: two full \
		 chunks of v1 rows fail Between's lower bound, and the reverse trim drops everything below a \
		 persistent cursor that has not finished walking down to it"
	);
}
