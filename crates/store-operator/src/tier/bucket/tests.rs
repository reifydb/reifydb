// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{keyspace::join::JoinLeft, state::GroupId},
		typed::direction::Asc,
	},
};
use reifydb_core::state::typed::SuffixBytes;
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use super::{BucketMap, write::TypedBucket};
use crate::types::DurablePre;

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
	bucket.record(GroupId(7), suffix(1), Some(row("written")), DurablePre::Absent);

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
	bucket.record(GroupId(7), suffix(1), Some(row("seven")), DurablePre::Absent);
	bucket.record(GroupId(9), suffix(1), Some(row("nine")), DurablePre::Absent);

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
		bucket.record(GroupId(7), suffix(n), Some(row(&format!("v{n}"))), DurablePre::Absent);
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
	bucket.record(GroupId(7), suffix(1), Some(row("live")), DurablePre::Absent);
	bucket.record(GroupId(7), suffix(1), None, DurablePre::Present(ByteSize::from_bytes(4)));

	let entry = bucket.get(GroupId(7), &suffix(1)).expect("a removal must stay visible until it is flushed");
	assert!(entry.post.is_none(), "a removal is a tombstone, not an absence; dropping it would resurrect the row");
	assert_eq!(
		entry.durable_pre,
		DurablePre::Absent,
		"the pre image describes what sqlite held when the key entered the bucket, never what a later \
		 write claimed; restating it makes the flush size a delete against a row the buffer itself wrote"
	);
}

#[test]
fn overwriting_a_suffix_does_not_count_it_twice() {
	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("first")), DurablePre::Absent);
	let after_first = bucket.footprint();
	bucket.record(GroupId(7), suffix(1), Some(row("first")), DurablePre::Absent);

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
	bucket.record(GroupId(7), suffix(1), Some(row("seven")), DurablePre::Absent);
	bucket.record(GroupId(9), suffix(1), Some(row("nine")), DurablePre::Absent);

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
		bucket.record(GroupId(7), suffix(n), Some(row("v")), DurablePre::Absent);
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

	map.bucket::<JoinLeft>(OP).record(GroupId(7), suffix(1), Some(row("first")), DurablePre::Absent);
	let entry = map.bucket::<JoinLeft>(OP).get(GroupId(7), &suffix(1)).expect("the second lookup must reach the first bucket");

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

	map.bucket::<JoinLeft>(OP).record(GroupId(7), suffix(1), Some(row("mine")), DurablePre::Absent);

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
	bucket.record(GroupId(7), suffix(1), Some(row("seven")), DurablePre::Absent);
	bucket.record(GroupId(9), suffix(2), Some(row("nine")), DurablePre::Absent);
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
	bucket.record(GroupId(7), suffix(1), Some(row("live")), DurablePre::Absent);
	bucket.flush(&conn).expect("first flush");

	bucket.record(GroupId(7), suffix(1), None, DurablePre::Present(ByteSize::from_bytes(4)));
	bucket.flush(&conn).expect("second flush");

	assert!(
		typed::scan::<JoinLeft>(&conn, OP).is_empty(),
		"a removal must delete the durable row; leaving it behind resurrects state the operator deleted"
	);
}

#[test]
fn a_flushed_row_survives_the_round_trip_through_its_payload() {
	use rusqlite::Connection;

	use super::AnyBucket;
	use reifydb_core::key::operator::traits::Keyspace;
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_value::util::cowvec::CowVec;

	use crate::tier::persistent::sqlite::{schema::ensure_schema, typed};

	let conn = Connection::open_in_memory().expect("in memory db");
	ensure_schema(&conn);

	let mut bucket = bucket();
	bucket.record(GroupId(7), suffix(1), Some(row("payload")), DurablePre::Absent);
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
		DurablePre::Absent,
	);

	let entry = map.bucket::<JoinLeft>(OP).get(GroupId(7), &suffix(1)).expect("a byte keyed write must land in the typed bucket");
	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec()).expect("utf8"),
		"erased",
		"the erased entry point is how every byte keyed caller reaches a bucket; if it misses, their writes vanish"
	);
}

#[test]
fn every_keyspace_in_the_catalogue_is_reachable_through_the_dispatch() {
	use reifydb_core::key::operator::keyspace::{KEYSPACES, KeyspaceVisitor, dispatch};
	use reifydb_core::key::operator::{state::KeyspaceId, traits::Keyspace};

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
	use reifydb_core::key::operator::traits::Keyspace;
	use std::ops::Bound;

	let mut map = BucketMap::default();
	for n in [3u64, 1, 2] {
		map.record_bytes(
			OP,
			JoinLeft::ID,
			GroupId(7),
			&suffix(n).to_suffix_bytes(),
			Some(row("v")),
			DurablePre::Absent,
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
	use reifydb_core::key::operator::traits::Keyspace;
	use std::ops::Bound;

	let mut map = BucketMap::default();
	for n in 0..5u64 {
		map.record_bytes(
			OP,
			JoinLeft::ID,
			GroupId(7),
			&suffix(n).to_suffix_bytes(),
			Some(row("v")),
			DurablePre::Absent,
		);
	}

	let page = map.page_bytes(OP, JoinLeft::ID, GroupId(7), Bound::Unbounded, Bound::Unbounded, Some(2));
	assert_eq!(page.len(), 2, "an unbounded page would blow the caller's budget on a large group");
}
