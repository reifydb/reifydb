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
use reifydb_store::tier::range::Materialize;
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use super::{BucketMap, TypedBucket};
use crate::tier::range::typed::KeyspaceRow;

impl KeyspaceRow for JoinLeft {
	type Row = EncodedPodRow;
}

const OP: OperatorId = OperatorId(1);

fn budget() -> ByteSize {
	ByteSize::from_mib(4)
}

fn bucket() -> TypedBucket<JoinLeft> {
	TypedBucket::<JoinLeft>::new(budget()).expect("a non zero budget must build a bucket")
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn suffix(n: u64) -> Asc<RowNumber> {
	Asc(RowNumber(n))
}

#[test]
fn two_groups_holding_the_same_suffix_never_read_each_other() {
	let bucket = bucket();

	for (group, body) in [(7u128, "seven"), (9, "nine")] {
		assert_eq!(
			bucket.materialize(OP, GroupId(group), &[(suffix(1), row(body))]),
			Materialize::Materialized,
			"each group must prove its own partition independently"
		);
	}

	for (group, expected) in [(7u128, "seven"), (9, "nine")] {
		let rows = bucket.group(OP, GroupId(group), 16).expect("a proven group must serve");
		assert_eq!(rows.len(), 1, "a group must see exactly its own row");
		assert_eq!(
			String::from_utf8(rows[0].1.body().to_vec()).expect("utf8"),
			expected,
			"a group must never read another group's row at the same suffix"
		);
	}
}

#[test]
fn a_write_into_an_unproven_group_is_refused() {
	let bucket = bucket();
	bucket.insert(OP, GroupId(7), suffix(1), row("unproven"));

	assert!(
		bucket.group(OP, GroupId(7), 16).is_none(),
		"a write the tier never proved must read back as a gap, not as data"
	);
}

#[test]
fn a_proven_group_serves_its_suffixes_in_ascending_order() {
	let bucket = bucket();
	let rows: Vec<_> = [3u64, 1, 2].iter().map(|n| (suffix(*n), row(&format!("v{n}")))).collect();
	let mut sorted = rows.clone();
	sorted.sort_by_key(|(key, _)| *key);

	assert_eq!(
		bucket.materialize(OP, GroupId(7), &sorted),
		Materialize::Materialized,
		"a whole group span must be cacheable"
	);

	let served = bucket.group(OP, GroupId(7), 16).expect("a proven group must serve");
	let order: Vec<u64> = served.iter().map(|(key, _)| key.0.0).collect();
	assert_eq!(order, vec![1, 2, 3], "a group must serve its suffixes in ascending row number order");
}

#[test]
fn the_bucket_map_hands_back_the_same_bucket_for_one_operator_and_keyspace() {
	let mut map = BucketMap::default();

	map.bucket::<JoinLeft>(OP, budget()).materialize(OP, GroupId(7), &[(suffix(1), row("first"))]);
	let rows = map.bucket::<JoinLeft>(OP, budget()).group(OP, GroupId(7), 16).expect("the group must be proven");

	assert_eq!(rows.len(), 1, "the second lookup must reach the bucket the first one filled");
	assert_eq!(String::from_utf8(rows[0].1.body().to_vec()).expect("utf8"), "first");
}
