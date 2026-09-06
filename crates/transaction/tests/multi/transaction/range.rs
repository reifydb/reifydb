// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{IndexId, TableId},
		object::ObjectId,
	},
	key::{any::AnyKey, bound::AnyKeyBoundRange, catalog::IndexEntryKey},
};
use reifydb_transaction::multi::RangeScope;

use super::test_multi;
use crate::{as_bound, as_key, as_values, from_bytes, multi::transaction::FromRow};

// String tails encode inverted, so every key in the 'a' group shares the tail prefix !a, and the
// group sorts in reverse of the plaintext.
fn prefix_range(prefix: u8) -> AnyKeyBoundRange {
	IndexEntryKey::key_prefix_range(ObjectId::Table(TableId(1)), IndexId::primary(1u64), &[!prefix])
}

#[test]
fn test_range() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();
	txn.commit(vec![]).unwrap();

	let four_to_one = AnyKeyBoundRange::start_end(as_bound!(4), as_bound!(1));

	let txn = engine.begin_query().unwrap();
	let items: Vec<_> =
		txn.range(four_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.bytes, as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> = txn.range_rev(four_to_one, RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.bytes, as_values!(expected));
		assert_eq!(v.version, 2);
	}
}

#[test]
fn test_range2() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let four_to_one = AnyKeyBoundRange::start_end(as_bound!(4), as_bound!(1));

	let items: Vec<_> =
		txn.range(four_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	let items: Vec<_> =
		txn.range_rev(four_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	txn.commit(vec![]).unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let seven_to_one = AnyKeyBoundRange::start_end(as_bound!(7), as_bound!(1));

	let items: Vec<_> =
		txn.range(seven_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=6).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> =
		txn.range_rev(seven_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=6).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 2);
	}
}

#[test]
fn test_range3() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let seven_to_four = AnyKeyBoundRange::start_end(as_bound!(7), as_bound!(4));

	let items: Vec<_> =
		txn.range(seven_to_four.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (4..=6).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	let items: Vec<_> =
		txn.range_rev(seven_to_four.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (4..=6).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	txn.commit(vec![]).unwrap();

	let five_to_one = AnyKeyBoundRange::start_end(as_bound!(5), as_bound!(1));

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let items: Vec<_> =
		txn.range(five_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=5).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> =
		txn.range_rev(five_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=5).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.bytes, &as_values!(expected));
		assert_eq!(v.version, 2);
	}
}

#[test]
fn test_range_edge() {
	// Version layout the reads below are checked against: a2, a3, b4 (del), b3, c2, c1.
	let engine = test_multi();

	// c1
	{
		let mut txn = engine.begin_command().unwrap();

		txn.set(&as_key!(0), as_values!(0u64)).unwrap();
		txn.set(&as_key!(u64::MAX), as_values!(u64::MAX)).unwrap();

		txn.set(&as_key!(3), as_values!(31u64)).unwrap();
		txn.commit(vec![]).unwrap();
		assert_eq!(2, engine.version().unwrap());
	}

	// a2, c2
	{
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(12u64)).unwrap();
		txn.set(&as_key!(3), as_values!(32u64)).unwrap();
		txn.commit(vec![]).unwrap();
		assert_eq!(3, engine.version().unwrap());
	}

	// b3
	{
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(13u64)).unwrap();
		txn.set(&as_key!(2), as_values!(23u64)).unwrap();
		txn.commit(vec![]).unwrap();
		assert_eq!(4, engine.version().unwrap());
	}

	// b4 (remove)
	{
		let mut txn = engine.begin_command().unwrap();
		txn.remove(&as_key!(2)).unwrap();
		txn.commit(vec![]).unwrap();
		assert_eq!(5, engine.version().unwrap());
	}

	let check_iter =
		|items: Vec<reifydb_core::interface::store::MultiVersionRow<reifydb_core::key::any::AnyKey>>,
		 expected: &[u64]| {
			let mut i = 0;
			for r in items {
				assert_eq!(expected[i], from_bytes!(u64, &r.bytes));
				i += 1;
			}
			assert_eq!(expected.len(), i);
		};

	let check_rev_iter =
		|items: Vec<reifydb_core::interface::store::MultiVersionRow<reifydb_core::key::any::AnyKey>>,
		 expected: &[u64]| {
			let mut i = 0;
			for r in items {
				assert_eq!(expected[i], from_bytes!(u64, &r.bytes));
				i += 1;
			}
			assert_eq!(expected.len(), i);
		};

	let ten_to_one = AnyKeyBoundRange::start_end(as_bound!(10), as_bound!(1));

	let mut txn = engine.begin_command().unwrap();
	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 13]);
	let items: Vec<_> =
		txn.range_rev(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 32]);

	txn.read_as_of_version_inclusive(CommitVersion(6)).unwrap();
	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	let mut count = 2;
	for v in items {
		if v.key == as_key!(1) {
			count -= 1;
		}

		if v.key == as_key!(3) {
			count -= 1;
		}
	}
	assert_eq!(0, count);

	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	let mut count = 2;
	for v in items {
		if v.key == as_key!(1) {
			count -= 1;
		}

		if v.key == as_key!(3) {
			count -= 1;
		}
	}
	assert_eq!(0, count);

	txn.read_as_of_version_inclusive(CommitVersion(4)).unwrap();
	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 23, 13]);

	let items: Vec<_> =
		txn.range_rev(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 23, 32]);

	txn.read_as_of_version_inclusive(CommitVersion(3)).unwrap();
	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 12]);

	let items: Vec<_> =
		txn.range_rev(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[12, 32]);

	txn.read_as_of_version_inclusive(CommitVersion(2)).unwrap();
	let items: Vec<_> =
		txn.range(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[31]);
	let items: Vec<_> =
		txn.range_rev(ten_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[31]);
}

#[test]
fn test_range_stream_returns_newest_version() {
	// A batch smaller than the key's version count must still yield the newest version; scanning
	// oldest-first would fill the batch with superseded versions and return a stale value.
	let engine = test_multi();

	const NUM_VERSIONS: u64 = 50;

	for i in 1..=NUM_VERSIONS {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(i)).unwrap();
		txn.commit(vec![]).unwrap();
	}

	let txn = engine.begin_query().unwrap();
	let items: Vec<_> =
		txn.range(AnyKeyBoundRange::all(), RangeScope::All, 5).collect::<Result<Vec<_>, _>>().unwrap();

	assert_eq!(items.len(), 1);
	let item = &items[0];
	assert_eq!(item.key, as_key!(1));
	assert_eq!(from_bytes!(u64, &item.bytes), NUM_VERSIONS);
}

#[test]
fn test_range_stream_multiple_keys_many_versions() {
	let engine = test_multi();

	const NUM_KEYS: u64 = 5;
	const VERSIONS_PER_KEY: u64 = 20;

	// Create many versions for each key
	for version in 1..=VERSIONS_PER_KEY {
		let mut txn = engine.begin_command().unwrap();
		for key in 1..=NUM_KEYS {
			// Value encodes both key and version for verification
			txn.set(&as_key!(key), as_values!(key * 1000 + version)).unwrap();
		}
		txn.commit(vec![]).unwrap();
	}

	// Query with streaming
	let txn = engine.begin_query().unwrap();
	let items: Vec<_> =
		txn.range(AnyKeyBoundRange::all(), RangeScope::All, 200).collect::<Result<Vec<_>, _>>().unwrap();

	// Should have all 5 keys, each with newest version
	// Keys are returned in descending order (5, 4, 3, 2, 1)
	assert_eq!(items.len(), 5);

	for (expected_key, item) in (1..=NUM_KEYS).rev().zip(items.iter()) {
		let expected_value = expected_key * 1000 + VERSIONS_PER_KEY;

		assert_eq!(item.key, as_key!(expected_key));
		assert_eq!(from_bytes!(u64, &item.bytes), expected_value);
	}
}

#[test]
fn a_prefix_range_sees_writes_the_transaction_has_not_committed_yet() {
	// A scan merges two sources: committed rows from storage, ordered by bytes, and the
	// transaction's own pending writes, ordered by AnyKeyBound. A prefix bound that orders
	// differently from its encoded form makes a transaction stop seeing its own writes while
	// committed rows keep arriving, which reads as data loss rather than as an error.
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_values!(1u64)).unwrap();
	txn.set(&as_key!("a2"), as_values!(2u64)).unwrap();
	txn.set(&as_key!("b1"), as_values!(3u64)).unwrap();

	let keys: Vec<AnyKey> = txn
		.range(prefix_range(b'a'), RangeScope::All, 1024)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|row| row.key)
		.collect();

	assert_eq!(keys, vec![as_key!("a2"), as_key!("a1")]);
}

#[test]
fn a_prefix_range_merges_uncommitted_writes_with_committed_rows() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_values!(1u64)).unwrap();
	txn.set(&as_key!("a3"), as_values!(3u64)).unwrap();
	txn.commit(vec![]).unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a2"), as_values!(2u64)).unwrap();

	let rows: Vec<_> = txn.range(prefix_range(b'a'), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();

	let keys: Vec<AnyKey> = rows.iter().map(|row| row.key.clone()).collect();
	assert_eq!(keys, vec![as_key!("a3"), as_key!("a2"), as_key!("a1")]);

	let values: Vec<u64> = rows.iter().map(|row| from_bytes!(u64, row.bytes)).collect();
	assert_eq!(values, vec![3, 2, 1]);
}

#[test]
fn a_prefix_range_hides_a_row_the_transaction_has_removed() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_values!(1u64)).unwrap();
	txn.set(&as_key!("a2"), as_values!(2u64)).unwrap();
	txn.commit(vec![]).unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.remove(&as_key!("a1")).unwrap();

	let keys: Vec<AnyKey> = txn
		.range(prefix_range(b'a'), RangeScope::All, 1024)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|row| row.key)
		.collect();

	assert_eq!(keys, vec![as_key!("a2")]);
}

#[test]
fn a_reverse_prefix_range_sees_uncommitted_writes_too() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_values!(1u64)).unwrap();
	txn.set(&as_key!("a2"), as_values!(2u64)).unwrap();
	txn.set(&as_key!("b1"), as_values!(3u64)).unwrap();

	let keys: Vec<AnyKey> = txn
		.range_rev(prefix_range(b'a'), RangeScope::All, 1024)
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
		.into_iter()
		.map(|row| row.key)
		.collect();

	assert_eq!(keys, vec![as_key!("a1"), as_key!("a2")]);
}
