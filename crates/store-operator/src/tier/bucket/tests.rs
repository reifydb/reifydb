// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::join::{JoinLeft, JoinRight},
			state::{GroupId, OperatorStateKey},
			traits::Keyspace,
		},
		typed::direction::Asc,
	},
	state::typed::SuffixBytes,
};
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use super::{BucketMap, write::TypedBucket};

const OP: OperatorId = OperatorId(1);

fn bucket() -> TypedBucket<JoinLeft> {
	TypedBucket::<JoinLeft>::new(OP)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn suffix(n: u64) -> Asc<RowNumber> {
	Asc(RowNumber(n))
}

#[test]
fn a_write_bucket_reads_back_what_it_recorded_without_being_proven() {
	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("written")));

	let entry = bucket.get(GroupId(7), &suffix(1)).expect("a recorded write must be readable at once");
	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"written",
		"the commit buffer is authoritative for its own writes; unlike the read cache it never reports a gap"
	);
}

#[test]
fn two_groups_in_one_write_bucket_never_read_each_other() {
	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("seven")));
	bucket.record(GroupId(9), suffix(1), Some(row("nine")));

	for (group, expected) in [(7u128, "seven"), (9, "nine")] {
		let entry = bucket.get(GroupId(group), &suffix(1)).expect("each group holds its own suffix");
		assert_eq!(
			String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
				.expect("utf8"),
			expected,
			"the group partitions the bucket, so one group's suffix must never answer for another's"
		);
	}
}

#[test]
fn a_write_bucket_ranges_its_suffixes_in_the_order_the_key_type_declares() {
	let mut bucket = bucket();
	for n in [3u64, 1, 2] {
		bucket.record(GroupId(7), suffix(n), Some(row(&format!("v{n}"))));
	}

	let order: Vec<u64> = bucket.range(GroupId(7), ..).map(|(key, _)| key.0.0).collect();
	assert_eq!(
		order,
		vec![1, 2, 3],
		"the flush merges the bucket against a sorted page, so out of order iteration corrupts the merge"
	);
}

#[test]
fn a_tombstone_is_recorded_rather_than_dropped() {
	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("live")));
	bucket.record(GroupId(7), suffix(1), None);

	let entry = bucket.get(GroupId(7), &suffix(1)).expect("a removal must stay visible until it is flushed");
	assert!(entry.post.is_none(), "a removal is a tombstone, not an absence; dropping it would resurrect the row");
}

#[test]
fn overwriting_a_suffix_does_not_count_it_twice() {
	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("first")));
	let after_first = bucket.footprint();
	bucket.record(GroupId(7), suffix(1), Some(row("first")));

	assert_eq!(bucket.len(), 1, "one suffix written twice is one row");
	assert_eq!(
		bucket.footprint(),
		after_first,
		"the flush budget is driven by the footprint, so double counting an overwrite starves it"
	);
}

#[test]
fn reaping_a_group_releases_only_that_group() {
	use super::{AnyBucket, Budget, Resume};

	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("seven")));
	bucket.record(GroupId(9), suffix(1), Some(row("nine")));

	let mut budget = Budget {
		rows: 16,
	};
	assert_eq!(bucket.reap_group(GroupId(7), &mut budget).expect("reap"), Resume::Done);

	assert!(bucket.get(GroupId(7), &suffix(1)).is_none(), "the reaped group must be gone");
	assert!(
		bucket.get(GroupId(9), &suffix(1)).is_some(),
		"a reap is scoped to one group; taking a neighbour's rows with it loses committed state"
	);
}

#[test]
fn a_reap_that_runs_out_of_budget_asks_to_be_resumed() {
	use super::{AnyBucket, Budget, Resume};

	let mut bucket = bucket();
	for n in 0..4u64 {
		bucket.record(GroupId(7), suffix(n), Some(row("v")));
	}

	let mut budget = Budget {
		rows: 2,
	};
	assert_eq!(
		bucket.reap_group(GroupId(7), &mut budget).expect("reap"),
		Resume::More,
		"a partially reaped group must report More or the caller drops the remainder on the floor"
	);
	assert_eq!(budget.rows, 0, "the reap must spend exactly the budget it was given");
	assert_eq!(bucket.len(), 2, "the unreaped half must still be there");
}

#[test]
fn the_bucket_map_hands_back_the_same_bucket_for_one_operator_and_keyspace() {
	let mut map = BucketMap::default();

	map.bucket::<JoinLeft>(OP).record(GroupId(7), suffix(1), Some(row("first")));
	let entry = map
		.bucket::<JoinLeft>(OP)
		.get(GroupId(7), &suffix(1))
		.expect("the second lookup must reach the first bucket");

	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"first"
	);
}

#[test]
fn two_operators_never_share_a_bucket() {
	let mut map = BucketMap::default();
	let other = OperatorId(2);

	map.bucket::<JoinLeft>(OP).record(GroupId(7), suffix(1), Some(row("mine")));

	assert!(
		map.bucket::<JoinLeft>(other).get(GroupId(7), &suffix(1)).is_none(),
		"the bucket is keyed on operator and keyspace, so one operator's state must never answer for another's"
	);
}

#[test]
fn a_flush_writes_every_group_into_the_keyspaces_own_table() {
	use rusqlite::Connection;

	use super::AnyBucket;
	use crate::tier::persistent::sqlite::{schema::ensure_schema, typed};

	let conn = Connection::open_in_memory().expect("in memory db");
	ensure_schema(&conn);

	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("seven")));
	bucket.record(GroupId(9), suffix(2), Some(row("nine")));
	bucket.flush(&conn).expect("flush");

	let rows = typed::scan::<JoinLeft>(&conn, OP);
	assert_eq!(rows.len(), 2, "every group in the bucket must reach the table, not just the first");
	assert!(bucket.is_empty(), "a flushed bucket must release its rows or the next flush writes them twice");
	assert_eq!(
		bucket.footprint(),
		ByteSize::ZERO,
		"the footprint drives the flush budget, so a flush that does not release it never lets the budget recover"
	);
}

#[test]
fn a_flushed_tombstone_deletes_the_row_rather_than_storing_a_none() {
	use rusqlite::Connection;

	use super::AnyBucket;
	use crate::tier::persistent::sqlite::{schema::ensure_schema, typed};

	let conn = Connection::open_in_memory().expect("in memory db");
	ensure_schema(&conn);

	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("live")));
	bucket.flush(&conn).expect("first flush");

	bucket.record(GroupId(7), suffix(1), None);
	bucket.flush(&conn).expect("second flush");

	assert!(
		typed::scan::<JoinLeft>(&conn, OP).is_empty(),
		"a removal must delete the durable row; leaving it behind resurrects state the operator deleted"
	);
}

#[test]
fn a_flushed_row_survives_the_round_trip_through_its_payload() {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::key::operator::traits::Keyspace;
	use reifydb_value::util::cowvec::CowVec;
	use rusqlite::Connection;

	use super::AnyBucket;
	use crate::tier::persistent::sqlite::{schema::ensure_schema, typed};

	let conn = Connection::open_in_memory().expect("in memory db");
	ensure_schema(&conn);

	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("payload")));
	bucket.flush(&conn).expect("flush");

	let stored = typed::get::<JoinLeft>(&conn, OP, &JoinLeft::join(GroupId(7), suffix(1))).expect("the row");
	let restored = EncodedPodRow::from(EncodedBytes(CowVec::new(stored)));
	assert_eq!(
		String::from_utf8(restored.body().to_vec()).expect("utf8"),
		"payload",
		"the payload round trip carries the pod header, so reading back the body alone would truncate the row"
	);
}

#[test]
fn an_erased_write_reaches_the_same_bucket_a_typed_one_does() {
	use reifydb_core::key::operator::traits::Keyspace;

	let mut map = BucketMap::default();
	map.record_bytes(
		OP,
		JoinLeft::ID,
		GroupId(7),
		&suffix(1).to_suffix_bytes(),
		Some(row("erased")),
	);

	let entry = map
		.bucket::<JoinLeft>(OP)
		.get(GroupId(7), &suffix(1))
		.expect("a byte keyed write must land in the typed bucket");
	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"erased",
		"the erased entry point is how every byte keyed caller reaches a bucket; if it misses, their writes vanish"
	);
}

#[test]
fn every_keyspace_in_the_catalogue_is_reachable_through_the_dispatch() {
	use reifydb_core::key::operator::{
		keyspace::{KEYSPACES, KeyspaceVisitor, dispatch},
		state::KeyspaceId,
		traits::Keyspace,
	};

	struct Name;

	impl KeyspaceVisitor for Name {
		type Output = &'static str;

		fn visit<K: Keyspace>(self) -> Self::Output {
			K::NAME
		}
	}

	for spec in KEYSPACES {
		assert_eq!(
			dispatch(spec.id, Name),
			Some(spec.name),
			"{} is in the catalogue but the dispatch cannot reach it, so every write to it would panic",
			spec.name
		);
	}

	assert_eq!(
		dispatch(KeyspaceId(0x00), Name),
		None,
		"an id no keyspace claims must report itself rather than answering as a neighbour"
	);
}

#[test]
fn an_erased_page_returns_its_suffixes_in_the_key_types_order() {
	use std::ops::Bound;

	use reifydb_core::key::operator::traits::Keyspace;

	let mut map = BucketMap::default();
	for n in [3u64, 1, 2] {
		map.record_bytes(
			OP,
			JoinLeft::ID,
			GroupId(7),
			&suffix(n).to_suffix_bytes(),
			Some(row("v")),
		);
	}

	let page = map.page_bytes(OP, JoinLeft::ID, GroupId(7), Bound::Unbounded, Bound::Unbounded, None);
	let order: Vec<Vec<u8>> = page.iter().map(|(suffix, _)| suffix.clone()).collect();
	let mut sorted = order.clone();
	sorted.sort();
	assert_eq!(
		order, sorted,
		"a page feeds a merge against a sorted durable page, so the erased path must preserve the typed order"
	);
	assert_eq!(order.len(), 3);
}

#[test]
fn an_erased_page_honours_its_limit() {
	use std::ops::Bound;

	use reifydb_core::key::operator::traits::Keyspace;

	let mut map = BucketMap::default();
	for n in 0..5u64 {
		map.record_bytes(
			OP,
			JoinLeft::ID,
			GroupId(7),
			&suffix(n).to_suffix_bytes(),
			Some(row("v")),
		);
	}

	let page = map.page_bytes(OP, JoinLeft::ID, GroupId(7), Bound::Unbounded, Bound::Unbounded, Some(2));
	assert_eq!(page.len(), 2, "an unbounded page would blow the caller's budget on a large group");
}

fn seeded_pair() -> BucketMap {
	// two groups crossed with two keyspaces is the smallest shape that can tell group-major order
	// apart from keyspace-major order; one group or one keyspace hides the difference entirely
	let mut map = BucketMap::default();
	for group in [GroupId(7), GroupId(9)] {
		for keyspace in [JoinLeft::ID, JoinRight::ID] {
			for n in [2u64, 1] {
				map.record_bytes(OP, keyspace, group, &suffix(n).to_suffix_bytes(), Some(row("v")));
			}
		}
	}
	map
}

fn expected_order() -> Vec<EncodedKey> {
	let mut keys = Vec::new();
	for group in [GroupId(7), GroupId(9)] {
		for keyspace in [JoinLeft::ID, JoinRight::ID] {
			for n in [1u64, 2] {
				keys.push(
					OperatorStateKey::inner_encoded(
						group,
						keyspace,
						suffix(n).to_suffix_bytes(),
					)
					.into_encoded(),
				);
			}
		}
	}
	keys.sort();
	keys
}

#[test]
fn a_scan_across_two_keyspaces_orders_by_group_before_keyspace() {
	// an inner key is [group][keyspace][suffix], so the group outranks the keyspace; iterating
	// keyspace by keyspace yields a different sequence and silently breaks the merge downstream,
	// which compares live against in-flight and relies on both sides agreeing exactly
	let map = seeded_pair();

	let scanned: Vec<EncodedKey> = map.encoded_entries(OP).into_iter().map(|(key, _)| key).collect();

	assert_eq!(
		scanned,
		expected_order(),
		"a scan that groups by keyspace first reorders every multi group operator, and the merge it \
		 feeds then misses its equal arm and serves a stale row"
	);
}

#[test]
fn a_range_spanning_two_keyspaces_pages_without_skipping_a_key() {
	// paging one key at a time must reconstruct the whole scan; a range that filters after
	// ordering by the wrong dimension drops keys between pages rather than failing loudly
	let map = seeded_pair();
	let whole: Vec<EncodedKey> =
		map.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded).into_iter().map(|(key, _)| key).collect();

	assert_eq!(whole, expected_order(), "an unbounded range must agree with an unbounded scan");

	let mut paged = Vec::new();
	let mut cursor = Bound::Unbounded;
	loop {
		let page = map.encoded_range(OP, &cursor, &Bound::Unbounded);
		let Some((key, _)) = page.first() else {
			break;
		};
		paged.push(key.clone());
		cursor = Bound::Excluded(key.clone());
	}

	assert_eq!(paged, expected_order(), "walking one key at a time must visit every key exactly once");
}

#[test]
fn a_bounded_range_keeps_the_keys_between_its_bounds_and_no_others() {
	// the bound is a whole encoded key, so a range that compares only the leading column would
	// admit neighbours from the wrong group or keyspace
	let map = seeded_pair();
	let all = expected_order();
	let lower = all[2].clone();
	let upper = all[5].clone();

	let seen: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Included(lower.clone()), &Bound::Excluded(upper.clone()))
		.into_iter()
		.map(|(key, _)| key)
		.collect();

	assert_eq!(seen, all[2..5].to_vec(), "an included start belongs to the range and an excluded end does not");
}

#[test]
fn a_reverse_scan_is_the_exact_mirror_of_a_forward_one() {
	// last_batch feeds merge_back, which is merge with its comparison flipped; if the forward and
	// reverse orders are not exact mirrors the two pagers disagree about what the last key is
	let map = seeded_pair();
	let forward: Vec<EncodedKey> =
		map.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded).into_iter().map(|(key, _)| key).collect();

	let mut reverse = forward.clone();
	reverse.reverse();
	let mut expected = expected_order();
	expected.reverse();

	assert_eq!(reverse, expected, "a reverse walk must mirror the forward one key for key");
}
