// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem::size_of,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::shape::ShapeId, store::EntryKind},
	key::{
		EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey,
		row::RowKey,
	},
	metrics::collect::MetricsCollector,
};
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{
	MultiVersionScope,
	tier::{
		RangeCursor, RawEntry, VersionedGetResult,
		read::{
			MultiReadBufferTier, ReadBufferConfig, ReadBufferDomainConfig, ReadBufferReadMetrics,
			ReadBufferWarmMetrics, ServedChunk,
		},
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

fn all_domains(resident_pages: usize, resident_bytes: ByteSize, shift: u8, shards: usize) -> MultiReadBufferTier {
	let domain = ReadBufferDomainConfig {
		resident_pages,
		resident_bytes,
		shards,
	};
	MultiReadBufferTier::new(ReadBufferConfig {
		operator: domain,
		general: domain,
		bucket_shift: shift,
	})
}

fn cache(resident_pages: usize) -> MultiReadBufferTier {
	all_domains(resident_pages, ByteSize::from_gib(1), DEFAULT_BUCKET_SHIFT, 1)
}

fn split_cache(operator: ReadBufferDomainConfig, general: ReadBufferDomainConfig, shift: u8) -> MultiReadBufferTier {
	MultiReadBufferTier::new(ReadBufferConfig {
		operator,
		general,
		bucket_shift: shift,
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
	all_domains(resident_pages, ByteSize::from_gib(1), shift, 1)
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

#[test]
fn supersede_keeps_the_previous_version_for_pinned_readers() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));

	match read.get(&key("k"), CommitVersion(10)) {
		VersionedGetResult::Value {
			value,
			version,
		} => {
			assert_eq!(value.as_ref(), b"v10");
			assert_eq!(version, CommitVersion(10));
		}
		other => panic!("reader at the current version must get the current slot, got {other:?}"),
	}

	match read.get(&key("k"), CommitVersion(7)) {
		VersionedGetResult::Value {
			value,
			version,
		} => {
			assert_eq!(value.as_ref(), b"v5", "reader between versions must be served previous");
			assert_eq!(version, CommitVersion(5));
		}
		other => panic!("expected the previous slot, got {other:?}"),
	}

	assert!(
		matches!(read.get(&key("k"), CommitVersion(4)), VersionedGetResult::NotFound),
		"a reader below both slots must still fall through"
	);
}

#[test]
fn previous_slot_serves_tombstones_in_both_directions() {
	let read = cache(8);
	read.insert(key("del-now"), CommitVersion(5), Some(val("v5")));
	read.insert(key("del-now"), CommitVersion(10), None);
	assert!(
		matches!(read.get(&key("del-now"), CommitVersion(12)), VersionedGetResult::Tombstone),
		"current tombstone serves readers at or above it"
	);
	match read.get(&key("del-now"), CommitVersion(7)) {
		VersionedGetResult::Value {
			value,
			..
		} => assert_eq!(value.as_ref(), b"v5", "reader below the tombstone gets the previous value"),
		other => panic!("expected the previous value below the tombstone, got {other:?}"),
	}

	read.insert(key("was-del"), CommitVersion(5), None);
	read.insert(key("was-del"), CommitVersion(10), Some(val("v10")));
	assert!(
		matches!(read.get(&key("was-del"), CommitVersion(7)), VersionedGetResult::Tombstone),
		"a previous-slot tombstone must serve as a definitive deletion"
	);
}

#[test]
fn flush_echo_clears_the_previous_slot() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));

	assert!(
		matches!(read.get(&key("k"), CommitVersion(7)), VersionedGetResult::NotFound),
		"after the flush echo the previous slot must be gone"
	);
	match read.get(&key("k"), CommitVersion(10)) {
		VersionedGetResult::Value {
			value,
			..
		} => assert_eq!(value.as_ref(), b"v10"),
		other => panic!("current slot must survive the echo, got {other:?}"),
	}
}

#[test]
fn older_insert_is_rejected_and_leaves_previous_intact() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));

	match read.get(&key("k"), CommitVersion(7)) {
		VersionedGetResult::Value {
			value,
			..
		} => assert_eq!(value.as_ref(), b"v5", "previous must survive an older re-insert"),
		other => panic!("expected previous to survive, got {other:?}"),
	}
	read.insert(key("k"), CommitVersion(3), Some(val("v3")));
	assert!(
		matches!(read.get(&key("k"), CommitVersion(4)), VersionedGetResult::NotFound),
		"an insert older than both slots must be rejected outright"
	);
}

#[test]
fn warm_replace_does_not_fabricate_a_previous_slot() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	read.insert(opkey(7, "a"), CommitVersion(5), Some(val("resident-v5")));
	read.populate_page(page, vec![opentry(7, "a", 10, "loaded-v10")], true);

	assert!(
		matches!(read.get(&opkey(7, "a"), CommitVersion(7)), VersionedGetResult::NotFound),
		"a warm replace must not invent adjacency between v5 and v10"
	);
}

#[test]
fn removal_drops_both_version_slots() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));
	read.remove_dropped(&key("k"));
	assert!(matches!(read.get(&key("k"), CommitVersion(7)), VersionedGetResult::NotFound));
	assert!(matches!(read.get(&key("k"), CommitVersion(10)), VersionedGetResult::NotFound));
}

#[test]
fn remove_dropped_through_removes_only_older_entries() {
	let read = cache(8);
	read.insert(key("old"), CommitVersion(5), Some(val("v5")));
	read.remove_dropped_through(&key("old"), CommitVersion(8));
	assert!(
		matches!(read.get(&key("old"), CommitVersion(9)), VersionedGetResult::NotFound),
		"an entry older than the drop version must be removed"
	);

	read.insert(key("new"), CommitVersion(10), Some(val("v10")));
	read.remove_dropped_through(&key("new"), CommitVersion(8));
	match read.get(&key("new"), CommitVersion(10)) {
		VersionedGetResult::Value {
			value,
			..
		} => assert_eq!(value.as_ref(), b"v10", "a recreated newer entry must survive the delayed drop"),
		other => panic!("expected the recreated entry to survive, got {other:?}"),
	}
}

#[test]
fn remove_dropped_through_clears_a_dropped_previous_slot() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(10), Some(val("v10")));
	read.remove_dropped_through(&key("k"), CommitVersion(8));

	assert!(
		matches!(read.get(&key("k"), CommitVersion(7)), VersionedGetResult::NotFound),
		"the dropped previous version must not be served"
	);
	match read.get(&key("k"), CommitVersion(10)) {
		VersionedGetResult::Value {
			value,
			..
		} => assert_eq!(value.as_ref(), b"v10"),
		other => panic!("current slot must survive, got {other:?}"),
	}
}

#[test]
fn remove_dropped_through_dirties_an_in_flight_warm() {
	let read = cache(8);
	let page = read.page_of_key(&opkey(7, "a"));
	assert!(read.begin_warm(page));
	read.remove_dropped_through(&opkey(7, "a"), CommitVersion(8));
	assert!(!read.finish_warm(page, vec![opentry(7, "a", 5, "stale")]), "a warm racing a delayed drop must abort");
}

#[test]
fn remove_dropped_through_removes_an_entry_at_exactly_the_drop_version() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(8), Some(val("v8")));
	read.remove_dropped_through(&key("k"), CommitVersion(8));
	assert!(
		matches!(read.get(&key("k"), CommitVersion(9)), VersionedGetResult::NotFound),
		"an entry at the drop version itself must be removed"
	);
}

fn cache_bytes(resident_pages: usize, resident_bytes: ByteSize, shift: u8) -> MultiReadBufferTier {
	all_domains(resident_pages, resident_bytes, shift, 1)
}

fn wide(len: usize) -> Option<CowVec<u8>> {
	Some(CowVec::new(vec![b'x'; len]))
}

#[test]
fn byte_budget_evicts_across_pages_even_when_page_count_is_within_cap() {
	let limit = ByteSize::from_kib(8);
	let read = cache_bytes(10_000, limit, 0);
	for n in 1..=64 {
		read.insert(row(1, n), CommitVersion(1), wide(1024));
	}
	assert!(
		read.resident_pages() < 64,
		"64 wide rows sit in 64 distinct pages, well under the 10_000 page cap, so only a byte cap can evict them"
	);
	assert!(
		read.resident_bytes().as_bytes() <= limit.as_bytes(),
		"resident bytes must stay within the byte budget: got {}, limit {}",
		read.resident_bytes(),
		limit
	);
}

#[test]
fn single_operator_page_is_bounded_by_bytes() {
	let limit = ByteSize::from_kib(8);
	let read = cache_bytes(1024, limit, DEFAULT_BUCKET_SHIFT);
	let mut total_inserted = 0u64;
	for n in 0..64 {
		read.insert(opkey(1, &format!("state-{n}")), CommitVersion(1), wide(1024));
		total_inserted += 1024;
	}
	assert!(
		read.resident_pages() <= 1,
		"every operator-state key of one node collapses to a single bucket-0 page, so page-count eviction can never fire"
	);
	assert!(
		total_inserted > limit.as_bytes(),
		"precondition: the workload must push more bytes than the budget, else the test proves nothing"
	);
	assert!(
		read.resident_bytes().as_bytes() <= limit.as_bytes(),
		"the previously-unbounded single operator page must now be byte-bounded: got {}, limit {}",
		read.resident_bytes(),
		limit
	);
}

#[test]
fn used_bytes_equal_sum_of_page_bytes_across_churn() {
	let read = cache_bytes(1024, ByteSize::from_gib(1), 0);
	read.insert(row(1, 1), CommitVersion(5), wide(200));
	read.insert(row(1, 1), CommitVersion(9), wide(300));
	read.insert(row(1, 1), CommitVersion(9), wide(120));
	read.insert(row(1, 2), CommitVersion(5), None);
	read.insert(row(1, 3), CommitVersion(5), wide(400));
	read.insert(row(1, 4), CommitVersion(5), wide(150));
	read.insert(row(1, 4), CommitVersion(9), wide(150));
	read.remove_dropped_through(&row(1, 4), CommitVersion(5));
	read.remove_dropped_through(&row(1, 3), CommitVersion(5));
	read.invalidate(&row(1, 1));
	assert_eq!(
		read.resident_bytes(),
		read.tallied_page_bytes(),
		"the budget counter must equal the sum of per-page tallies; any drift means a mutation site mis-accounted"
	);
}

#[test]
fn releasing_every_entry_returns_used_to_zero() {
	let read = cache_bytes(1024, ByteSize::from_gib(1), 0);
	for n in 1..=8 {
		read.insert(row(1, n), CommitVersion(1), wide(500));
	}
	assert!(read.resident_bytes().as_bytes() > 0, "inserts must charge the budget");
	for n in 1..=8 {
		read.invalidate(&row(1, n));
	}
	assert_eq!(read.resident_bytes(), ByteSize::ZERO, "removing every entry must fully reclaim the byte budget");
	assert_eq!(read.resident_pages(), 0, "emptied pages must be dropped, not retained at zero bytes");
}

#[test]
fn byte_budget_eviction_prefers_evicting_probationary_pages_over_hot_ones() {
	let limit = ByteSize::from_kib(8);
	let read = cache_bytes(10_000, limit, 0);
	read.insert(row(1, 1), CommitVersion(1), wide(1024));
	assert!(matches!(read.get(&row(1, 1), CommitVersion(1)), VersionedGetResult::Value { .. }));
	for n in 2..=64 {
		read.insert(row(1, n), CommitVersion(1), wide(1024));
	}
	assert!(
		matches!(read.get(&row(1, 1), CommitVersion(1)), VersionedGetResult::Value { .. }),
		"the hot page must survive byte-budget eviction even though it is the oldest resident page"
	);
	assert!(
		read.resident_bytes().as_bytes() <= limit.as_bytes(),
		"byte budget must still be enforced while the hot page is preserved: got {}, limit {}",
		read.resident_bytes(),
		limit
	);
}

fn cache_bytes_sharded(
	resident_pages: usize,
	resident_bytes: ByteSize,
	shift: u8,
	shards: usize,
) -> MultiReadBufferTier {
	all_domains(resident_pages, resident_bytes, shift, shards)
}

#[test]
fn multi_shard_byte_budget_is_enforced_independently_per_shard() {
	let limit = ByteSize::from_kib(8);
	let read = cache_bytes_sharded(10_000, limit, 0, 4);
	for n in 1..=256 {
		read.insert(row(1, n), CommitVersion(1), wide(1024));
	}
	assert!(
		read.resident_bytes().as_bytes() <= limit.as_bytes(),
		"the sum of every shard's used bytes must stay within the total configured budget: got {}, limit {}",
		read.resident_bytes(),
		limit
	);
}

#[test]
fn source_pressure_never_evicts_a_complete_operator_page() {
	let read = split_cache(
		ReadBufferDomainConfig {
			resident_pages: 1024,
			resident_bytes: ByteSize::from_gib(1),
			shards: 1,
		},
		ReadBufferDomainConfig {
			resident_pages: 1024,
			resident_bytes: ByteSize::from_kib(8),
			shards: 1,
		},
		DEFAULT_BUCKET_SHIFT,
	);
	let op_page = read.page_of_key(&opkey(7, "a"));
	read.populate_page(op_page, vec![opentry(7, "a", 5, "a")], true);
	assert!(read.page_is_complete(op_page), "operator page must start complete");

	for n in 1..=64 {
		read.insert(row(1, n), CommitVersion(1), wide(1024));
	}

	assert!(
		matches!(read.get(&row(1, 1), CommitVersion(1)), VersionedGetResult::NotFound),
		"precondition: the source flood must exceed the general byte budget and evict its own pages, \
		 otherwise the test proves nothing"
	);
	assert!(
		read.page_is_complete(op_page),
		"a complete operator page must survive source-domain byte-budget eviction: the budgets are separate accounts"
	);
	assert!(
		matches!(read.get(&opkey(7, "a"), CommitVersion(9)), VersionedGetResult::Value { .. }),
		"the operator value must still be served from memory after the source flood"
	);
	assert!(
		matches!(read.get(&opkey(7, "missing"), CommitVersion(9)), VersionedGetResult::Tombstone),
		"operator absence must still be served from the complete page after the source flood"
	);
}

#[test]
fn operator_pressure_never_evicts_a_source_page() {
	let read = split_cache(
		ReadBufferDomainConfig {
			resident_pages: 1024,
			resident_bytes: ByteSize::from_kib(8),
			shards: 1,
		},
		ReadBufferDomainConfig {
			resident_pages: 1024,
			resident_bytes: ByteSize::from_gib(1),
			shards: 1,
		},
		DEFAULT_BUCKET_SHIFT,
	);
	read.insert(row(1, 0), CommitVersion(1), wide(2048));
	assert!(matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }));

	let mut total = 0u64;
	for n in 0..64 {
		read.insert(opkey(1, &format!("state-{n:03}")), CommitVersion(1), wide(1024));
		total += 1024;
	}
	assert!(
		total > ByteSize::from_kib(8).as_bytes(),
		"precondition: operator inserts must exceed the operator byte budget so its own domain evicts"
	);
	assert!(
		matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }),
		"a resident source page must survive operator-domain byte-budget pressure: separate budgets"
	);
}

#[test]
fn metrics_collector_attributes_bytes_to_the_owning_domain() {
	let read = cache(8);
	read.insert(opkey(1, "a"), CommitVersion(1), wide(512));
	read.insert(row(1, 0), CommitVersion(1), wide(256));

	let mut samples = Vec::new();
	read.collect(&mut samples);

	let value = |scope: &str, metric: &str| -> f64 {
		samples.iter()
			.find(|s| s.scope == scope && s.metric == metric)
			.map(|s| s.reading.as_f64())
			.unwrap_or_else(|| panic!("sample {scope}/{metric} must be reported"))
	};

	assert_eq!(
		value("read_buffer::operator", "resident_bytes"),
		read.operator_resident_bytes().as_bytes() as f64,
		"reported operator bytes must equal the live accessor"
	);
	assert_eq!(value("read_buffer::operator", "resident_pages"), 1.0, "one operator page is resident");
	assert_eq!(value("read_buffer::general", "resident_pages"), 1.0, "one source page is resident");
	assert!(
		value("read_buffer::operator", "resident_bytes") >= 512.0,
		"the operator entry's bytes must be attributed to the operator domain"
	);
	assert!(
		value("read_buffer::general", "resident_bytes") >= 256.0,
		"the source entry's bytes must be attributed to the general domain"
	);
	assert!(
		value("read_buffer::general", "resident_bytes") < value("read_buffer::operator", "resident_bytes"),
		"the 512-byte operator entry must NOT leak into the general domain's tally; per-entry overhead \
		 is shared, so the domain holding the larger value must tally strictly heavier"
	);
	assert_eq!(
		value("read_buffer::operator", "resident_bytes") + value("read_buffer::general", "resident_bytes"),
		read.resident_bytes().as_bytes() as f64,
		"the two domain tallies must partition the whole pool's resident bytes (nothing dropped or double-counted)"
	);
}

#[test]
fn read_buffer_operator_metrics_groups_resident_and_payload_bytes_by_flow_node() {
	let read = cache(1024);
	read.insert(opkey(7, "a"), CommitVersion(1), Some(val("aaaa")));
	read.insert(opkey(7, "b"), CommitVersion(1), Some(val("bb")));
	read.insert(FlowNodeInternalStateKey::encoded(7u64, b"i".to_vec()), CommitVersion(1), Some(val("ii")));
	read.insert(opkey(9, "c"), CommitVersion(1), Some(val("cccc")));
	read.insert(row(1, 1), CommitVersion(1), Some(val("general")));

	let per_node = read.operator_metrics();
	let nodes: Vec<u64> = per_node.iter().map(|usage| usage.node.0).collect();
	assert_eq!(nodes, vec![7, 9], "exactly the two flow nodes with resident operator state, sorted by id");

	let resident_total: u64 = per_node.iter().map(|usage| usage.resident.as_bytes()).sum();
	assert_eq!(
		resident_total,
		read.operator_resident_bytes().as_bytes(),
		"per-node attribution must account for every operator-domain byte (and only those)"
	);
	let payload_total: u64 = per_node.iter().map(|usage| usage.payload.as_bytes()).sum();
	assert_eq!(
		payload_total,
		read.operator_payload_bytes().as_bytes(),
		"per-node payload attribution must partition the domain-wide payload tally the same way"
	);

	let usage_of = |wanted: u64| {
		per_node.iter()
			.find(|usage| usage.node.0 == wanted)
			.unwrap_or_else(|| panic!("node {wanted} must be resident"))
	};
	let version = size_of::<CommitVersion>() as u64;
	assert_eq!(
		usage_of(9).payload.as_bytes(),
		opkey(9, "c").len() as u64 + version + 4,
		"payload must be exactly key + version + value bytes, the same formula the disk measurement \
		 sums per row, so the two metrics are directly comparable"
	);
	assert!(
		usage_of(9).resident.as_bytes() > usage_of(9).payload.as_bytes(),
		"resident carries per-entry struct overhead on top of payload and must tally strictly heavier"
	);
	assert!(
		usage_of(7).resident.as_bytes() > usage_of(9).resident.as_bytes(),
		"node 7 holds three entries (two state + one internal) and must tally strictly heavier than \
		 node 9's single entry; equal tallies would mean internal state or multi-entry pages are dropped"
	);
}

#[test]
fn read_buffer_operator_metrics_reflects_live_pages_not_stale_counters() {
	let read = cache(1024);
	read.insert(opkey(7, "a"), CommitVersion(1), Some(val("aaaa")));
	read.insert(opkey(7, "b"), CommitVersion(1), Some(val("bb")));

	let before = read.operator_metrics();
	assert_eq!(before.len(), 1);
	let before_resident = before[0].resident.as_bytes();
	let before_payload = before[0].payload.as_bytes();

	read.remove_dropped(&opkey(7, "a"));
	let after = read.operator_metrics();
	assert_eq!(after.len(), 1, "node 7 still has one resident entry");
	assert!(
		after[0].resident.as_bytes() < before_resident,
		"removing a state entry must shrink the node's attributed resident bytes immediately"
	);
	assert_eq!(
		before_payload - after[0].payload.as_bytes(),
		opkey(7, "a").len() as u64 + size_of::<CommitVersion>() as u64 + 4,
		"the payload delta of a removal must be exactly the removed entry's key + version + value bytes"
	);

	read.remove_dropped(&opkey(7, "b"));
	assert!(
		read.operator_metrics().is_empty(),
		"a node with no resident operator state must not appear at all (no ghost zero-byte rows)"
	);
}

#[test]
fn superseded_entry_payload_counts_both_versions_like_disk_rows() {
	let read = cache(1024);
	let version = size_of::<CommitVersion>() as u64;
	read.insert(opkey(3, "k"), CommitVersion(5), Some(val("first")));
	let single = read.operator_metrics()[0].payload.as_bytes();
	assert_eq!(single, opkey(3, "k").len() as u64 + version + 5);

	read.insert(opkey(3, "k"), CommitVersion(9), Some(val("second!")));
	let both = read.operator_metrics()[0].payload.as_bytes();
	assert_eq!(
		both,
		2 * (opkey(3, "k").len() as u64 + version) + 5 + 7,
		"a supersede keeps the previous version resident; payload must count key + version once per \
		 version because the persistent tier stores one row per version"
	);
}

#[test]
fn payload_accounting_survives_supersede_echo_and_removal_churn() {
	let read = cache(1024);
	let version = size_of::<CommitVersion>() as u64;
	read.insert(opkey(4, "a"), CommitVersion(5), Some(val("aaa")));
	read.insert(opkey(4, "a"), CommitVersion(9), Some(val("bbbbb")));
	read.insert(opkey(4, "a"), CommitVersion(9), Some(val("bbbbb")));
	read.insert(opkey(4, "b"), CommitVersion(5), Some(val("cc")));
	read.insert(opkey(4, "b"), CommitVersion(9), Some(val("d")));
	read.remove_dropped_through(&opkey(4, "b"), CommitVersion(5));
	read.insert(opkey(4, "gone"), CommitVersion(5), Some(val("x")));
	read.remove_dropped(&opkey(4, "gone"));

	let per_node = read.operator_metrics();
	assert_eq!(per_node.len(), 1);
	let expected = (opkey(4, "a").len() as u64 + version + 5) + (opkey(4, "b").len() as u64 + version + 1);
	assert_eq!(
		per_node[0].payload.as_bytes(),
		expected,
		"after a supersede, a flush echo (clears previous), a delayed drop of a previous slot, and a \
		 full removal, the payload counter must equal exactly the surviving versions' bytes; any drift \
		 means a mutation site mis-accounted payload"
	);
}

#[test]
fn metrics_collector_publishes_payload_bytes_per_domain() {
	let read = cache(8);
	read.insert(opkey(1, "a"), CommitVersion(1), wide(512));
	read.insert(row(1, 0), CommitVersion(1), wide(256));

	let mut samples = Vec::new();
	read.collect(&mut samples);

	let value = |scope: &str, metric: &str| -> f64 {
		samples.iter()
			.find(|s| s.scope == scope && s.metric == metric)
			.map(|s| s.reading.as_f64())
			.unwrap_or_else(|| panic!("sample {scope}/{metric} must be reported"))
	};

	assert_eq!(
		value("read_buffer::operator", "payload_bytes"),
		read.operator_payload_bytes().as_bytes() as f64,
		"reported operator payload must equal the live accessor"
	);
	assert_eq!(
		value("read_buffer::general", "payload_bytes"),
		read.general_payload_bytes().as_bytes() as f64,
		"reported general payload must equal the live accessor"
	);
	assert!(
		value("read_buffer::operator", "payload_bytes") < value("read_buffer::operator", "resident_bytes"),
		"payload excludes per-entry struct overhead and must be strictly below resident in the same domain"
	);
	assert!(
		value("read_buffer::general", "payload_bytes") < value("read_buffer::general", "resident_bytes"),
		"payload excludes per-entry struct overhead and must be strictly below resident in the same domain"
	);
}

fn sum_reads(read: &MultiReadBufferTier, domain: &str) -> ReadBufferReadMetrics {
	let mut total = ReadBufferReadMetrics::default();
	for metrics in read.shard_metrics().into_iter().filter(|m| m.domain == domain) {
		total.point_hits += metrics.reads.point_hits;
		total.previous_hits += metrics.reads.previous_hits;
		total.point_misses += metrics.reads.point_misses;
		total.range_served += metrics.reads.range_served;
		total.range_gaps += metrics.reads.range_gaps;
	}
	total
}

fn sum_warms(read: &MultiReadBufferTier, domain: &str) -> ReadBufferWarmMetrics {
	let mut total = ReadBufferWarmMetrics::default();
	for metrics in read.shard_metrics().into_iter().filter(|m| m.domain == domain) {
		total.warms_started += metrics.warms.warms_started;
		total.warms_completed += metrics.warms.warms_completed;
		total.warms_dirty_aborted += metrics.warms.warms_dirty_aborted;
		total.warms_aborted += metrics.warms.warms_aborted;
		total.pages_warm_blocked += metrics.warms.pages_warm_blocked;
		total.pages_evicted += metrics.warms.pages_evicted;
		total.complete_pages_invalidated += metrics.warms.complete_pages_invalidated;
	}
	total
}

#[test]
fn point_read_outcomes_are_tallied_as_hits_previous_hits_and_misses() {
	let read = cache(8);
	read.insert(key("k"), CommitVersion(5), Some(val("v5")));
	read.insert(key("k"), CommitVersion(9), Some(val("v9")));
	read.insert(key("t"), CommitVersion(3), None);

	assert!(matches!(read.get(&key("k"), CommitVersion(9)), VersionedGetResult::Value { .. }));
	assert!(matches!(read.get(&key("k"), CommitVersion(6)), VersionedGetResult::Value { .. }));
	assert!(matches!(read.get(&key("t"), CommitVersion(3)), VersionedGetResult::Tombstone));
	assert!(matches!(read.get(&key("absent"), CommitVersion(1)), VersionedGetResult::NotFound));
	assert!(matches!(read.get(&key("k"), CommitVersion(4)), VersionedGetResult::NotFound));

	assert_eq!(
		sum_reads(&read, "general"),
		ReadBufferReadMetrics {
			point_hits: 2,
			previous_hits: 1,
			point_misses: 2,
			range_served: 0,
			range_gaps: 0,
		},
		"current-slot serve and cached tombstone are hits, the superseded slot is a previous hit, \
		 and both the absent key and the version-bound fall-through are misses"
	);
	assert_eq!(sum_reads(&read, "operator"), ReadBufferReadMetrics::default(), "no operator keys were read");
}

#[test]
fn range_serve_outcomes_are_tallied_as_served_and_gaps() {
	let read = cache(8);
	let entry = raw_entry(1, 5, 1, "v");
	let page = read.page_of_key(&entry.key);
	read.populate_page(page, vec![entry], false);

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
	let after_gap = sum_reads(&read, "general");
	assert_eq!((after_gap.range_gaps, after_gap.range_served), (1, 0), "an incomplete page is a gap");

	let served_read = cache(8);
	populate_complete(&served_read, 1, &[(0u64, 1u64, "a"), (5, 1, "b")]);
	let served = serve_collect(
		&served_read,
		1,
		0,
		10,
		MultiVersionScope::AsOf {
			read: CommitVersion(10),
		},
		16,
		false,
	);
	assert_eq!(served.len(), 2, "the complete page must serve both rows");
	let after_serve = sum_reads(&served_read, "general");
	assert_eq!((after_serve.range_served, after_serve.range_gaps), (1, 0), "a complete page is a serve");
}

#[test]
fn warm_lifecycle_counters_track_each_outcome_separately() {
	let read = cache(8);
	let entry = raw_entry(1, 5, 1, "v");
	let page = read.page_of_key(&entry.key);

	assert!(read.begin_warm(page));
	assert!(read.finish_warm(page, vec![raw_entry(1, 5, 1, "v")]));
	assert!(read.page_is_complete(page));

	assert!(read.begin_warm(page));
	read.invalidate(&entry.key);
	assert!(!read.finish_warm(page, vec![raw_entry(1, 5, 1, "v")]), "a write during the warm discards it");

	assert!(read.begin_warm(page));
	read.abort_warm(page);

	read.set_warm_blocked(page);

	assert_eq!(
		sum_warms(&read, "general"),
		ReadBufferWarmMetrics {
			warms_started: 3,
			warms_completed: 1,
			warms_dirty_aborted: 1,
			warms_aborted: 1,
			pages_warm_blocked: 1,
			pages_evicted: 0,
			complete_pages_invalidated: 1,
		},
		"one clean warm, one dirty discard, one abort, one block mark, and the invalidate \
		 that broke the completed page"
	);
}

#[test]
fn budget_evictions_are_counted_per_evicted_page() {
	let read = cache(1);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	read.insert(row(2, 0), CommitVersion(1), Some(val("b")));

	assert_eq!(read.resident_pages(), 1, "the page bound must hold");
	assert_eq!(sum_warms(&read, "general").pages_evicted, 1, "exactly one page was evicted for capacity");
}

#[test]
fn shard_metrics_reports_state_gauges_per_shard_and_domain() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b")]);
	assert!(matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }));

	let metrics = read.shard_metrics();
	assert_eq!(metrics.len(), 2, "one shard per domain configured, so exactly two rows");

	let general = metrics.iter().find(|m| m.domain == "general").expect("general shard row");
	assert_eq!(general.shard, 0);
	assert_eq!(general.state.pages, 1, "both rows land in the same bucket");
	assert_eq!(general.state.entries, 2);
	assert_eq!(general.state.complete_pages, 1);
	assert_eq!(general.state.hot_pages, 1, "the point hit marked the page hot");
	assert_eq!(general.state.blocked_pages, 0);
	assert_eq!(general.state.warming, 0);
	assert!(general.state.used.as_bytes() > 0);
	assert_eq!(general.state.limit, ByteSize::from_gib(1), "single shard owns the whole domain budget");

	let operator = metrics.iter().find(|m| m.domain == "operator").expect("operator shard row");
	assert_eq!(operator.state.pages, 0, "no operator keys were touched");
}
