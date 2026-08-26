// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem::size_of,
	ops::Bound,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::storage::StorageId, store::EntryKind},
	key::{EncodableKey, row::RowKey},
	metrics::collect::MetricsCollector,
};
use reifydb_store::row::page::{DEFAULT_BUCKET_SHIFT, PageId};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{
	MultiVersionScope,
	tier::{
		RangeCursor, RawEntry, VersionedGetResult,
		read::{
			MultiReadBufferTier, ReadBufferConfig, ReadBufferPageMetrics, ReadBufferReadMetrics,
			ServedChunk,
		},
	},
};

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn val(s: &str) -> CowVec<u8> {
	CowVec::new(s.as_bytes().to_vec())
}

fn row(storage: u64, n: u64) -> EncodedKey {
	RowKey {
		storage: StorageId::table(storage),
		row: RowNumber(n),
	}
	.encode()
}

fn buffer(resident_pages: usize, resident_bytes: ByteSize, shift: u8, shards: usize) -> MultiReadBufferTier {
	MultiReadBufferTier::new(ReadBufferConfig {
		resident_pages,
		resident_bytes: Some(resident_bytes),
		shards,
		bucket_shift: shift,
	})
	.unwrap()
}

fn cache(resident_pages: usize) -> MultiReadBufferTier {
	buffer(resident_pages, ByteSize::from_gib(1), DEFAULT_BUCKET_SHIFT, 1)
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

	for (object, payload) in [(1u64, "a"), (2, "b"), (3, "c")] {
		if let VersionedGetResult::Value {
			value: v,
			..
		} = read.get(&row(object, 0), CommitVersion(1))
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
	buffer(resident_pages, ByteSize::from_gib(1), shift, 1)
}

fn source(storage: u64) -> EntryKind {
	EntryKind::Source(StorageId::table(storage))
}

fn raw_entry(object: u64, n: u64, version: u64, value: &str) -> RawEntry {
	RawEntry {
		key: row(object, n),
		version: CommitVersion(version),
		value: Some(CowVec::new(value.as_bytes().to_vec())),
	}
}

fn materialize_page(read: &MultiReadBufferTier, page: PageId, mut entries: Vec<RawEntry>) {
	let range = read.page_key_range(page).expect("a table row page has a reconstructable range");
	let (Bound::Included(lo), Bound::Included(through)) = (range.start, range.end) else {
		panic!("a table row page range is inclusive at both ends");
	};
	entries.sort_by(|left, right| left.key.cmp(&right.key));
	assert!(
		read.materialize_scanned_chunk(page.kind, &lo, &through, &entries),
		"a page chunk must publish its claim"
	);
}

fn populate_complete(read: &MultiReadBufferTier, object: u64, rows: &[(u64, u64, &str)]) {
	let mut by_page: HashMap<PageId, Vec<RawEntry>> = HashMap::new();
	for (n, v, val) in rows {
		let entry = raw_entry(object, *n, *v, val);
		by_page.entry(read.page_of_key(&entry.key)).or_default().push(entry);
	}
	for (page, entries) in by_page {
		materialize_page(read, page, entries);
	}
}

fn serve_collect(
	read: &MultiReadBufferTier,
	object: u64,
	lo_row: u64,
	hi_row: u64,
	scope: MultiVersionScope,
	batch: usize,
	descending: bool,
) -> Vec<RawEntry> {
	let start = row(object, hi_row);
	let end = row(object, lo_row);
	let table = EntryKind::Source(StorageId::table(object));
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
				if cursor.is_exhausted() {
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
	let page = read.page_of_key(&row(1, 5));
	read.insert(row(1, 5), CommitVersion(1), Some(val("v")));
	assert!(!read.page_is_complete(page));

	let table = EntryKind::Source(StorageId::table(1));
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
	assert!(cursor.last_key().is_none() && !cursor.is_exhausted(), "Gap must leave the cursor untouched");
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
fn serve_stops_at_incomplete_bucket_after_a_complete_one() {
	let read = cache_shift(64, 4);
	let rows: Vec<(u64, u64, &str)> = (16u64..32).map(|n| (n, 1u64, "x")).collect();
	populate_complete(&read, 1, &rows);
	assert!(read.page_is_complete(read.page_of_key(&row(1, 16))));
	assert!(!read.page_is_complete(read.page_of_key(&row(1, 0))));

	let table = EntryKind::Source(StorageId::table(1));
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
	assert!(!cursor.is_exhausted(), "the incomplete bucket still remains");

	let last_before = cursor.last_key().cloned();
	let gap = read.serve_persistent_chunk(table, &mut cursor, start.as_slice(), end.as_slice(), scope, 100, false);
	assert!(matches!(gap, ServedChunk::Gap), "the incomplete bucket must read through");
	assert_eq!(cursor.last_key(), last_before.as_ref(), "Gap must not advance the cursor");
	assert!(!cursor.is_exhausted());
}

#[test]
fn populate_non_source_page_is_never_complete() {
	let read = cache(8);
	let key = EncodedKey::new(vec![0u8, 1, 2]);
	read.insert(key.clone(), CommitVersion(1), Some(CowVec::new(b"v".to_vec())));

	let page = read.page_of_key(&key);
	assert_eq!(page.kind, EntryKind::Multi, "the key must land outside the source band");
	assert!(!read.page_is_complete(page), "a non-Source page can never be covered");
}

#[test]
fn populate_respects_stale_version_guard() {
	let read = cache(8);
	let k = row(1, 5);
	let page = read.page_of_key(&k);
	materialize_page(
		&read,
		page,
		vec![RawEntry {
			key: k.clone(),
			version: CommitVersion(5),
			value: Some(CowVec::new(b"v5".to_vec())),
		}],
	);
	materialize_page(
		&read,
		page,
		vec![RawEntry {
			key: k.clone(),
			version: CommitVersion(2),
			value: Some(CowVec::new(b"v2".to_vec())),
		}],
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
fn invalidate_clears_the_page_coverage() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b")]);
	let page = read.page_of_key(&row(1, 0));
	assert!(read.page_is_complete(page));

	read.invalidate(&row(1, 5));
	assert!(!read.page_is_complete(page), "invalidating a key must clear its bucket's completeness");
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
fn a_replace_does_not_fabricate_a_previous_slot() {
	let read = cache(8);
	let page = read.page_of_key(&row(1, 5));
	read.insert(row(1, 5), CommitVersion(5), Some(val("resident-v5")));
	materialize_page(&read, page, vec![raw_entry(1, 5, 10, "loaded-v10")]);

	assert!(
		matches!(read.get(&row(1, 5), CommitVersion(7)), VersionedGetResult::NotFound),
		"a replace must not invent adjacency between v5 and v10"
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
fn a_drop_landing_inside_a_materialize_refuses_its_claim() {
	let read = MultiReadBufferTier::with_interlock(
		ReadBufferConfig {
			resident_pages: 8,
			resident_bytes: Some(ByteSize::from_gib(1)),
			shards: 1,
			bucket_shift: DEFAULT_BUCKET_SHIFT,
		},
		Box::new(|tier, _page| tier.remove_dropped_through(&row(1, 5), CommitVersion(8))),
	)
	.unwrap();

	let published =
		read.materialize_scanned_chunk(source(1), &row(1, 10), &row(1, 0), &[raw_entry(1, 5, 5, "stale")]);

	assert!(!published, "a materialize whose token was falsified mid-fill must publish nothing");
	assert!(!read.covers(source(1), &row(1, 5)), "the refused claim landed anyway");
	assert!(!read.covers(source(1), &row(1, 7)), "the refused claim landed anyway");
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
	buffer(resident_pages, resident_bytes, shift, 1)
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
	buffer(resident_pages, resident_bytes, shift, shards)
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
fn superseded_entry_payload_counts_both_resident_versions() {
	let read = cache(1024);
	let version = size_of::<CommitVersion>() as u64;
	read.insert(row(3, 1), CommitVersion(5), Some(val("first")));
	let single = read.payload_bytes().as_bytes();
	assert_eq!(single, row(3, 1).len() as u64 + version + 5);

	read.insert(row(3, 1), CommitVersion(9), Some(val("second!")));
	let both = read.payload_bytes().as_bytes();
	assert_eq!(
		both,
		2 * (row(3, 1).len() as u64 + version) + 5 + 7,
		"a supersede keeps the previous version resident; payload must count key + version once per \
		 version because a reader below the newer version is served from the previous slot"
	);
}

#[test]
fn payload_accounting_survives_supersede_echo_and_removal_churn() {
	let read = cache(1024);
	let version = size_of::<CommitVersion>() as u64;
	read.insert(row(4, 1), CommitVersion(5), Some(val("aaa")));
	read.insert(row(4, 1), CommitVersion(9), Some(val("bbbbb")));
	read.insert(row(4, 1), CommitVersion(9), Some(val("bbbbb")));
	read.insert(row(4, 2), CommitVersion(5), Some(val("cc")));
	read.insert(row(4, 2), CommitVersion(9), Some(val("d")));
	read.remove_dropped_through(&row(4, 2), CommitVersion(5));
	read.insert(row(4, 3), CommitVersion(5), Some(val("x")));
	read.remove_dropped(&row(4, 3));

	let expected = (row(4, 1).len() as u64 + version + 5) + (row(4, 2).len() as u64 + version + 1);
	assert_eq!(
		read.payload_bytes().as_bytes(),
		expected,
		"after a supersede, a flush echo (clears previous), a delayed drop of a previous slot, and a \
		 full removal, the payload counter must equal exactly the surviving versions' bytes; any drift \
		 means a mutation site mis-accounted payload"
	);
}

#[test]
fn metrics_collector_publishes_payload_bytes() {
	let read = cache(8);
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
		value("read_buffer", "payload_bytes"),
		read.payload_bytes().as_bytes() as f64,
		"reported payload must equal the live accessor"
	);
	assert!(
		value("read_buffer", "payload_bytes") < value("read_buffer", "resident_bytes"),
		"payload excludes per-entry struct overhead and must be strictly below resident"
	);
}

fn sum_reads(read: &MultiReadBufferTier) -> ReadBufferReadMetrics {
	let mut total = ReadBufferReadMetrics::default();
	for metrics in read.shard_metrics() {
		total.point_hits += metrics.reads.point_hits;
		total.previous_hits += metrics.reads.previous_hits;
		total.point_misses += metrics.reads.point_misses;
		total.range_served += metrics.reads.range_served;
		total.range_gaps += metrics.reads.range_gaps;
	}
	total
}

fn sum_pages(read: &MultiReadBufferTier) -> ReadBufferPageMetrics {
	let mut total = ReadBufferPageMetrics::default();
	for metrics in read.shard_metrics() {
		total.pages_evicted += metrics.pages.pages_evicted;
		total.complete_pages_invalidated += metrics.pages.complete_pages_invalidated;
	}
	total
}

fn sum_materializes(read: &MultiReadBufferTier) -> (u64, u64, u64) {
	let mut published = 0;
	let mut rows = 0;
	let mut refused = 0;
	for metrics in read.shard_metrics() {
		published += metrics.coverage.materializes;
		rows += metrics.coverage.materialize_rows;
		refused += metrics.coverage.materializes_refused;
	}
	(published, rows, refused)
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
		sum_reads(&read),
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
}

#[test]
fn range_serve_outcomes_are_tallied_as_served_and_gaps() {
	let read = cache(8);
	read.insert(row(1, 5), CommitVersion(1), Some(val("v")));

	let table = EntryKind::Source(StorageId::table(1));
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
	let after_gap = sum_reads(&read);
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
	let after_serve = sum_reads(&served_read);
	assert_eq!((after_serve.range_served, after_serve.range_gaps), (1, 0), "a complete page is a serve");
}

#[test]
fn materialize_outcomes_are_tallied_as_published_rows_and_refusals() {
	let read = MultiReadBufferTier::with_interlock(
		ReadBufferConfig {
			resident_pages: 8,
			resident_bytes: Some(ByteSize::from_gib(1)),
			shards: 1,
			bucket_shift: DEFAULT_BUCKET_SHIFT,
		},
		Box::new(|tier, _page| {
			if tier.covers(source(1), &row(1, 5)) {
				tier.invalidate(&row(1, 5));
			}
		}),
	)
	.unwrap();

	assert!(read.materialize_scanned_chunk(
		source(1),
		&row(1, 65535),
		&row(1, 0),
		&[raw_entry(1, 5, 1, "v"), raw_entry(1, 7, 1, "w")]
	));
	assert_eq!(sum_materializes(&read), (1, 2, 0), "the first materialize published both rows it was handed");

	assert!(!read.materialize_scanned_chunk(source(1), &row(1, 65535), &row(1, 0), &[raw_entry(1, 5, 2, "v2")]));
	assert_eq!(
		sum_materializes(&read),
		(1, 2, 1),
		"the invalidate inside the second fill must refuse its claim, and a refused claim must not count \
		 its rows as published"
	);
	assert_eq!(
		sum_pages(&read).complete_pages_invalidated,
		1,
		"the invalidate broke a page one claim spanned entirely"
	);
}

#[test]
fn budget_evictions_are_counted_per_evicted_page() {
	let read = cache(1);
	read.insert(row(1, 0), CommitVersion(1), Some(val("a")));
	read.insert(row(2, 0), CommitVersion(1), Some(val("b")));

	assert_eq!(read.resident_pages(), 1, "the page bound must hold");
	assert_eq!(sum_pages(&read).pages_evicted, 1, "exactly one page was evicted for capacity");
}

#[test]
fn shard_metrics_reports_state_gauges_per_shard() {
	let read = cache(8);
	populate_complete(&read, 1, &[(0u64, 1u64, "a"), (5, 1, "b")]);
	assert!(matches!(read.get(&row(1, 0), CommitVersion(1)), VersionedGetResult::Value { .. }));

	let metrics = read.shard_metrics();
	assert_eq!(metrics.len(), 1, "one shard configured, so exactly one row");

	let only_shard = &metrics[0];
	assert_eq!(only_shard.shard, 0);
	assert_eq!(only_shard.state.pages, 1, "both rows land in the same bucket");
	assert_eq!(only_shard.state.entries, 2);
	assert_eq!(only_shard.state.complete_pages, 1);
	assert_eq!(only_shard.state.hot_pages, 1, "the point hit marked the page hot");
	assert!(only_shard.state.used.as_bytes() > 0);
	assert_eq!(only_shard.state.limit, ByteSize::from_gib(1), "single shard owns the whole buffer budget");
}

#[test]
fn complete_page_hides_writes_newer_than_the_read_scope() {
	let read = cache_shift(64, 4);
	let rows: Vec<(u64, u64, &str)> = (0u64..4).map(|n| (n, 100u64, "new")).collect();
	populate_complete(&read, 1, &rows);
	let page = read.page_of_key(&row(1, 0));
	assert!(read.page_is_complete(page), "fixture must produce a complete page");

	let table = EntryKind::Source(StorageId::table(1));
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(50),
	};
	let (start, end) = (row(1, 3), row(1, 0));
	let mut cursor = RangeCursor::new();
	let chunk =
		read.serve_persistent_chunk(table, &mut cursor, start.as_slice(), end.as_slice(), scope, 100, false);

	let ServedChunk::Served(batch) = chunk else {
		panic!(
			"a complete page owns every key in the range and must serve it, never yield a gap to persistent"
		);
	};
	assert_eq!(
		batch.entries.len(),
		0,
		"every resident row is newer than the read scope, so none of them may be visible to this reader"
	);
	assert!(
		cursor.is_exhausted(),
		"the page covers the whole range, so the scan must end rather than resume in persistent"
	);

	assert!(
		matches!(read.get(&row(1, 0), CommitVersion(50)), VersionedGetResult::NotFound),
		"with no version at or below the scope the point path must report NotFound so the caller falls through"
	);
}
