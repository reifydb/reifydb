// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::shape::ShapeId, store::EntryKind},
	key::{
		EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey,
		row::RowKey,
	},
};
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{
	MultiVersionScope,
	tier::{
		RangeCursor, RawEntry, VersionedGetResult,
		read::{MultiReadBufferTier, ReadBufferConfig, ServedChunk},
	},
};

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

fn val(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn row(shape: u64, n: u64) -> EncodedKey {
	RowKey {
		shape: ShapeId::table(shape),
		row: RowNumber(n),
	}
	.encode()
}

fn cache(resident_pages: usize) -> MultiReadBufferTier {
	MultiReadBufferTier::new(ReadBufferConfig {
		resident_pages,
		bucket_shift: DEFAULT_BUCKET_SHIFT,
		shards: 1,
	})
}

#[test]
fn insert_then_get_returns_value_when_version_high_enough() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	match read.get(&key("k"), CommitVersion(5)) {
		VersionedGetResult::Value {
			value: v,
			version: ver,
		} => {
			assert_eq!(v.as_ref(), b"v5");
			assert_eq!(ver, CommitVersion(5));
		}
		VersionedGetResult::Tombstone => panic!("expected value, got tombstone"),
		VersionedGetResult::NotFound => panic!("expected hit at exactly stored version"),
	}

	assert!(matches!(read.get(&key("k"), CommitVersion(9)), VersionedGetResult::Value { .. }));
}

#[test]
fn get_below_stored_version_misses_so_caller_reads_through() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	assert!(
		matches!(read.get(&key("k"), CommitVersion(4)), VersionedGetResult::NotFound),
		"must miss below the stored version"
	);
}

#[test]
fn tombstone_is_cached_and_served() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(3), None);
	assert!(matches!(read.get(&key("k"), CommitVersion(3)), VersionedGetResult::Tombstone));
}

#[test]
fn invalidate_removes_the_key() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(1), Some(val("v1")));
	read.invalidate(&key("k"));
	assert!(
		matches!(read.get(&key("k"), CommitVersion(1)), VersionedGetResult::NotFound),
		"invalidated key must miss"
	);
}

#[test]
fn newer_insert_overwrites_but_older_insert_is_ignored() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));

	read.insert(key("k"), CommitVersion(2), Some(val("v2")));
	match read.get(&key("k"), CommitVersion(5)) {
		VersionedGetResult::Value {
			value: v,
			..
		} => assert_eq!(v.as_ref(), b"v5", "older insert must not overwrite"),
		VersionedGetResult::Tombstone => panic!("unexpected tombstone"),
		VersionedGetResult::NotFound => panic!("unexpected miss"),
	}

	read.insert(key("k"), CommitVersion(7), Some(val("v7")));
	match read.get(&key("k"), CommitVersion(7)) {
		VersionedGetResult::Value {
			value: v,
			version: ver,
		} => {
			assert_eq!(v.as_ref(), b"v7");
			assert_eq!(ver, CommitVersion(7));
		}
		VersionedGetResult::Tombstone => panic!("unexpected tombstone"),
		VersionedGetResult::NotFound => panic!("unexpected miss"),
	}
}

#[test]
fn eviction_bounds_page_count_and_never_changes_correctness() {
	let read = cache(2);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	read.insert(row(2, 0), CommitVersion(1), Some(val("b")));
	read.insert(row(3, 0), CommitVersion(1), Some(val("c")));
	assert!(read.resident_pages() <= 2, "read buffer must stay within the page bound");

	for (shape, payload) in [(1u64, "a"), (2, "b"), (3, "c")] {
		if let VersionedGetResult::Value {
			value: v,
			..
		} = read.get(&row(shape, 0), CommitVersion(1))
		{
			assert_eq!(v.as_ref(), payload.as_bytes());
		}
	}
}

#[test]
fn scan_resistant_eviction_keeps_hot_working_set() {
	let read = cache(2);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	assert!(matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }));
	read.insert(row(2, 0), CommitVersion(1), Some(val("b")));
	read.insert(row(3, 0), CommitVersion(1), Some(val("c")));
	assert!(
		matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }),
		"the hot (twice-accessed) page must survive a probationary flood"
	);
	assert!(
		matches!(read.get(&row(2, 0), CommitVersion(1)), VersionedGetResult::NotFound),
		"the oldest probationary page must be evicted first"
	);
	assert!(matches!(read.get(&row(3, 0), CommitVersion(1)), VersionedGetResult::Value { .. }));
}

#[test]
fn growing_capacity_preserves_pages() {
	let read = cache(2);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	read.insert(row(2, 0), CommitVersion(1), Some(val("b")));
	read.set_capacity(8);
	for shape in [1u64, 2] {
		assert!(
			matches!(read.get(&row(shape, 0), CommitVersion(1)), VersionedGetResult::Value { .. }),
			"page must survive a capacity grow"
		);
	}
}

#[test]
fn shrinking_capacity_evicts_only_excess() {
	let read = cache(8);
	for shape in [1u64, 2, 3, 4] {
		read.insert(row(shape, 0), CommitVersion(1), Some(val("x")));
	}
	read.set_capacity(2);
	assert_eq!(read.resident_pages(), 2, "shrink must leave exactly the new page capacity, not clear the cache");
}

#[test]
fn reconfigure_clears_cache_and_applies_new_capacity() {
	let read = cache(8);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	assert_eq!(read.resident_pages(), 1);
	read.reconfigure(2, 64);
	assert_eq!(read.resident_pages(), 0, "reconfigure must clear pages because the page size changed");

	read.insert(row(1, 0), CommitVersion(2), Some(val("a2")));
	match read.get(&row(1, 0), CommitVersion(2)) {
		VersionedGetResult::Value {
			value: v,
			..
		} => assert_eq!(v.as_ref(), b"a2"),
		_ => panic!("expected the repopulated value after reconfigure"),
	}
}

#[test]
fn clone_shares_backing_storage() {
	let a = cache(4);
	let b = a.clone();
	a.insert(key("k"), CommitVersion(1), Some(val("v")));
	assert!(
		matches!(b.get(&key("k"), CommitVersion(1)), VersionedGetResult::Value { .. }),
		"clone observes writes from the original"
	);
}

fn cache_shift(resident_pages: usize, shift: u8) -> MultiReadBufferTier {
	MultiReadBufferTier::new(ReadBufferConfig {
		resident_pages,
		bucket_shift: shift,
		shards: 1,
	})
}

fn raw_entry(shape: u64, n: u64, version: u64, value: &str) -> RawEntry {
	RawEntry {
		key: row(shape, n),
		version: CommitVersion(version),
		value: Some(CowVec::new(value.as_bytes().to_vec())),
	}
}

fn populate_complete(read: &MultiReadBufferTier, shape: u64, rows: &[(u64, u64, &str)]) {
	let mut by_page: HashMap<PageId, Vec<RawEntry>> = HashMap::new();
	for (n, v, val) in rows {
		let entry = raw_entry(shape, *n, *v, val);
		by_page.entry(read.page_of_key(&entry.key)).or_default().push(entry);
	}
	for (page, entries) in by_page {
		read.populate_page(page, entries, true);
	}
}

fn serve_collect(
	read: &MultiReadBufferTier,
	shape: u64,
	lo_row: u64,
	hi_row: u64,
	scope: MultiVersionScope,
	batch: usize,
	descending: bool,
) -> Vec<RawEntry> {
	let start = row(shape, hi_row);
	let end = row(shape, lo_row);
	let table = EntryKind::Source(ShapeId::table(shape));
	let mut cursor = RangeCursor::new();
	let mut out = Vec::new();
	for _ in 0..10_000 {
		match read.serve_persistent_chunk(
			table,
			&mut cursor,
			start.as_slice(),
			end.as_slice(),
			scope,
			batch,
			descending,
		) {
			ServedChunk::Served(batch) => {
				out.extend(batch.entries);
				if cursor.exhausted {
					break;
				}
			}
			ServedChunk::Gap => break,
		}
	}
	out
}

#[test]
fn serve_complete_bucket_returns_rows_in_ascending_encoded_order() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b"), (10, 1, "c"), (3, 1, "d")]);

	let served = serve_collect(
		&read,
		1,
		0,
		10,
		MultiVersionScope::AsOf {
			read: CommitVersion(10),
		},
		16,
		false,
	);

	let keys: Vec<EncodedKey> = served.iter().map(|e| e.key.clone()).collect();
	let mut expected: Vec<EncodedKey> = [0u64, 3, 5, 10].iter().map(|n| row(1, *n)).collect();
	expected.sort();
	assert_eq!(keys, expected, "serve must yield in-range keys in ascending-encoded order");
}

#[test]
fn serve_returns_gap_when_bucket_not_complete() {
	let read = cache(8);
	let entry = raw_entry(1, 5, 1, "v");
	let page = read.page_of_key(&entry.key);
	read.populate_page(page, vec![entry], false);
	assert!(!read.page_is_complete(page));

	let table = EntryKind::Source(ShapeId::table(1));
	let (start, end) = (row(1, 10), row(1, 0));
	let mut cursor = RangeCursor::new();
	let result = read.serve_persistent_chunk(
		table,
		&mut cursor,
		start.as_slice(),
		end.as_slice(),
		MultiVersionScope::AsOf {
			read: CommitVersion(10),
		},
		16,
		false,
	);
	assert!(matches!(result, ServedChunk::Gap));
	assert!(cursor.last_key.is_none() && !cursor.exhausted, "Gap must leave the cursor untouched");
}

#[test]
fn serve_filters_by_scope() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (1, 5, "b"), (2, 10, "c")]);

	let mut asof: Vec<u64> = serve_collect(
		&read,
		1,
		0,
		2,
		MultiVersionScope::AsOf {
			read: CommitVersion(7),
		},
		16,
		false,
	)
	.iter()
	.map(|e| e.version.0)
	.collect();
	asof.sort();
	assert_eq!(asof, vec![1, 5], "AsOf{{7}} admits versions 1 and 5, excludes 10");

	let mut between: Vec<u64> = serve_collect(
		&read,
		1,
		0,
		2,
		MultiVersionScope::Between {
			after: CommitVersion(1),
			read: CommitVersion(10),
		},
		16,
		false,
	)
	.iter()
	.map(|e| e.version.0)
	.collect();
	between.sort();
	assert_eq!(between, vec![5, 10], "Between(1,10] excludes version 1 via the lower bound");
}

#[test]
fn serve_excludes_keys_outside_the_range() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b"), (10, 1, "c"), (20, 1, "d")]);

	let served = serve_collect(
		&read,
		1,
		5,
		10,
		MultiVersionScope::AsOf {
			read: CommitVersion(1),
		},
		16,
		false,
	);
	let keys: HashSet<EncodedKey> = served.iter().map(|e| e.key.clone()).collect();

	assert_eq!(served.len(), 2, "only rows 5 and 10 fall inside [5,10]");
	assert!(keys.contains(&row(1, 5)) && keys.contains(&row(1, 10)));
	assert!(!keys.contains(&row(1, 0)) && !keys.contains(&row(1, 20)), "rows outside the range must be excluded");
}

#[test]
fn serve_paginates_without_dups_or_gaps() {
	let read = cache(8);
	let rows: Vec<(u64, u64, &str)> = (0u64..10).map(|n| (n, 1u64, "x")).collect();
	populate_complete(&read, 1, &rows);

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(1),
	};
	let small: Vec<EncodedKey> =
		serve_collect(&read, 1, 0, 9, scope, 3, false).into_iter().map(|e| e.key).collect();
	let big: Vec<EncodedKey> =
		serve_collect(&read, 1, 0, 9, scope, 100, false).into_iter().map(|e| e.key).collect();

	assert_eq!(small, big, "small-batch pagination must match single-batch order");
	assert_eq!(small.len(), 10);
	assert_eq!(small.iter().collect::<HashSet<_>>().len(), 10, "no duplicate keys");
}

#[test]
fn serve_steps_across_consecutive_complete_buckets() {
	let read = cache_shift(64, 4);
	let rows: Vec<(u64, u64, &str)> = (0u64..32).map(|n| (n, 1u64, "x")).collect();
	populate_complete(&read, 1, &rows);
	assert!(read.page_is_complete(read.page_of_key(&row(1, 0))));
	assert!(read.page_is_complete(read.page_of_key(&row(1, 16))));

	let served = serve_collect(
		&read,
		1,
		0,
		31,
		MultiVersionScope::AsOf {
			read: CommitVersion(1),
		},
		5,
		false,
	);
	let keys: Vec<EncodedKey> = served.iter().map(|e| e.key.clone()).collect();
	let mut expected: Vec<EncodedKey> = (0u64..32).map(|n| row(1, n)).collect();
	expected.sort();
	assert_eq!(keys, expected, "cross-bucket serve must yield every row once, in ascending-encoded order");
}

#[test]
fn serve_stops_at_incomplete_bucket_after_a_complete_one() {
	let read = cache_shift(64, 4);
	let rows: Vec<(u64, u64, &str)> = (16u64..32).map(|n| (n, 1u64, "x")).collect();
	populate_complete(&read, 1, &rows);
	assert!(read.page_is_complete(read.page_of_key(&row(1, 16))));
	assert!(!read.page_is_complete(read.page_of_key(&row(1, 0))));

	let table = EntryKind::Source(ShapeId::table(1));
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(1),
	};
	let (start, end) = (row(1, 31), row(1, 0));
	let mut cursor = RangeCursor::new();

	let first =
		read.serve_persistent_chunk(table, &mut cursor, start.as_slice(), end.as_slice(), scope, 100, false);
	match first {
		ServedChunk::Served(batch) => {
			assert_eq!(batch.entries.len(), 16, "serves the whole complete bucket")
		}
		ServedChunk::Gap => panic!("expected Served for the complete bucket"),
	}
	assert!(!cursor.exhausted, "the incomplete bucket still remains");

	let last_before = cursor.last_key.clone();
	let gap = read.serve_persistent_chunk(table, &mut cursor, start.as_slice(), end.as_slice(), scope, 100, false);
	assert!(matches!(gap, ServedChunk::Gap), "the incomplete bucket must read through");
	assert_eq!(cursor.last_key, last_before, "Gap must not advance the cursor");
	assert!(!cursor.exhausted);
}

#[test]
fn serve_reverse_is_forward_reversed() {
	let read = cache_shift(64, 4);
	let rows: Vec<(u64, u64, &str)> = (0u64..32).map(|n| (n, 1u64, "x")).collect();
	populate_complete(&read, 1, &rows);

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(1),
	};
	let forward: Vec<EncodedKey> =
		serve_collect(&read, 1, 0, 31, scope, 5, false).into_iter().map(|e| e.key).collect();
	let mut reverse: Vec<EncodedKey> =
		serve_collect(&read, 1, 0, 31, scope, 5, true).into_iter().map(|e| e.key).collect();
	reverse.reverse();

	assert_eq!(forward, reverse, "reverse serve must be the exact reverse of forward serve");
	assert_eq!(forward.len(), 32);
}

#[test]
fn populate_non_source_page_is_never_complete() {
	let read = cache(8);
	let page = PageId {
		kind: EntryKind::Multi,
		bucket: 0,
	};
	read.populate_page(
		page,
		vec![RawEntry {
			key: EncodedKey::new(vec![0u8, 1, 2]),
			version: CommitVersion(1),
			value: Some(CowVec::new(b"v".to_vec())),
		}],
		true,
	);
	assert!(!read.page_is_complete(page), "a non-Source page can never be range_complete");
}

#[test]
fn populate_respects_stale_version_guard() {
	let read = cache(8);
	let k = row(1, 5);
	let page = read.page_of_key(&k);
	read.populate_page(
		page,
		vec![RawEntry {
			key: k.clone(),
			version: CommitVersion(5),
			value: Some(CowVec::new(b"v5".to_vec())),
		}],
		true,
	);
	read.populate_page(
		page,
		vec![RawEntry {
			key: k.clone(),
			version: CommitVersion(2),
			value: Some(CowVec::new(b"v2".to_vec())),
		}],
		true,
	);
	match read.get(&k, CommitVersion(5)) {
		VersionedGetResult::Value {
			value,
			version,
		} => {
			assert_eq!(value.as_ref(), b"v5", "older populate must not overwrite a newer entry");
			assert_eq!(version, CommitVersion(5));
		}
		_ => panic!("expected the newer value to survive"),
	}
}

#[test]
fn invalidate_clears_range_complete() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b")]);
	let page = read.page_of_key(&row(1, 0));
	assert!(read.page_is_complete(page));

	read.invalidate(&row(1, 5));
	assert!(!read.page_is_complete(page), "invalidating a key must clear its bucket's completeness");
}

fn opkey(node: u64, suffix: &str) -> EncodedKey {
	FlowNodeStateKey::encoded(node, suffix.as_bytes().to_vec())
}

fn opentry(node: u64, suffix: &str, version: u64, value: &str) -> RawEntry {
	RawEntry {
		key: opkey(node, suffix),
		version: CommitVersion(version),
		value: Some(val(value)),
	}
}

#[test]
fn complete_operator_page_serves_definitive_absence() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	read.populate_page(page, vec![opentry(7, "a", 5, "a")], true);

	assert!(
		matches!(read.get(&opkey(7, "missing"), CommitVersion(9)), VersionedGetResult::Tombstone),
		"absence on a complete page must be definitive"
	);
}

#[test]
fn incomplete_operator_page_does_not_claim_absence() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	read.populate_page(page, vec![opentry(7, "a", 5, "a")], false);

	assert!(
		matches!(read.get(&opkey(7, "missing"), CommitVersion(9)), VersionedGetResult::NotFound),
		"an incomplete page cannot prove absence and must fall through"
	);
}

#[test]
fn remove_dropped_keeps_completeness_while_invalidate_clears_it() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	read.populate_page(page, vec![opentry(7, "a", 5, "a"), opentry(7, "b", 5, "b")], true);

	read.remove_dropped(&opkey(7, "a"));
	assert!(read.page_is_complete(page), "a drop after the persistent delete must keep completeness");
	assert!(matches!(read.get(&opkey(7, "a"), CommitVersion(9)), VersionedGetResult::Tombstone));

	read.invalidate(&opkey(7, "b"));
	assert!(!read.page_is_complete(page), "invalidate must stay conservative and clear completeness");
}

#[test]
fn dirtied_warm_aborts_and_does_not_resurrect_dropped_state() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	assert!(read.begin_warm(page));

	read.remove_dropped(&opkey(7, "a"));

	assert!(
		!read.finish_warm(page, vec![opentry(7, "a", 5, "stale")]),
		"a warm dirtied by a concurrent removal must abort"
	);
	assert!(!read.page_is_complete(page));
	assert!(
		matches!(read.get(&opkey(7, "a"), CommitVersion(9)), VersionedGetResult::NotFound),
		"the stale bulk-loaded entry must not be resurrected"
	);
}

#[test]
fn clean_warm_marks_the_page_complete() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	assert!(read.begin_warm(page));
	assert!(read.finish_warm(page, vec![opentry(7, "a", 5, "a")]));
	assert!(read.page_is_complete(page));
}

#[test]
fn concurrent_warm_of_the_same_page_is_rejected_until_settled() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	assert!(read.begin_warm(page));
	assert!(!read.begin_warm(page), "a second warm of the same page must be rejected while in flight");
	read.abort_warm(page);
	assert!(read.begin_warm(page), "an aborted warm must free the slot");
}

#[test]
fn warm_blocked_page_is_not_a_candidate() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	assert!(read.page_is_warm_candidate(page));
	read.set_warm_blocked(page);
	assert!(!read.page_is_warm_candidate(page));
	assert!(!read.page_is_complete(page));
}

#[test]
fn state_and_internal_state_of_one_node_use_distinct_complete_pages() {
	let read = cache(8);
	let state = read.page_of_key(&opkey(7, "a"));
	let internal = read.page_of_key(&FlowNodeInternalStateKey::encoded(7u64, b"a".to_vec()));
	assert_ne!(state, internal);
	assert!(read.page_key_range(state).is_some(), "state pages must have a completable key range");
	assert!(read.page_key_range(internal).is_some(), "internal pages must have a completable key range");
}
