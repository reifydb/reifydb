// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{CommitVersion, EncodedKeyRange};
use reifydb_transaction::multi::{
	TransactionMulti,
	transaction::{range::TransactionRangeIter, range_rev::TransactionRangeRevIter},
};

use crate::{as_key, as_values, from_values, multi::transaction::FromValues};

#[tokio::test]
async fn test_range() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();
	txn.commit().unwrap();

	let four_to_one = EncodedKeyRange::start_end(Some(as_key!(4)), Some(as_key!(1)));

	let txn = engine.begin_query().await.unwrap();
	let iter = txn.range(four_to_one.clone()).unwrap();
	for (expected, v) in (1..=3).rev().zip(iter) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.values, as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let iter = txn.range_rev(four_to_one).unwrap();
	for (expected, v) in (1..=3).zip(iter) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.values, as_values!(expected));
		assert_eq!(v.version, 2);
	}
}

#[tokio::test]
async fn test_range2() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let four_to_one = EncodedKeyRange::start_end(Some(as_key!(4)), Some(as_key!(1)));

	let iter = txn.range(four_to_one.clone()).unwrap();
	for (expected, v) in (1..=3).rev().zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 1);
	}

	let iter = txn.range_rev(four_to_one.clone()).unwrap();
	for (expected, v) in (1..=3).zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 1);
	}

	txn.commit().unwrap();

	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let seven_to_one = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(1)));

	let iter = txn.range(seven_to_one.clone()).unwrap();
	for (expected, v) in (1..=6).rev().zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 2);
	}

	let iter = txn.range_rev(seven_to_one.clone()).unwrap();
	for (expected, v) in (1..=6).zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 2);
	}
}

#[tokio::test]
async fn test_range3() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let seven_to_four = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(4)));

	let iter = txn.range(seven_to_four.clone()).unwrap();
	for (expected, v) in (4..=6).rev().zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 1);
	}

	let iter = txn.range_rev(seven_to_four.clone()).unwrap();
	for (expected, v) in (4..=6).zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 1);
	}

	txn.commit().unwrap();

	let five_to_one = EncodedKeyRange::start_end(Some(as_key!(5)), Some(as_key!(1)));

	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let iter = txn.range(five_to_one.clone()).unwrap();
	for (expected, v) in (1..=5).rev().zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 2);
	}

	let iter = txn.range_rev(five_to_one.clone()).unwrap();
	for (expected, v) in (1..=5).zip(iter) {
		assert_eq!(v.key(), &as_key!(expected));
		assert_eq!(v.values(), &as_values!(expected));
		assert_eq!(v.version(), 2);
	}
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[tokio::test]
async fn test_range_edge() {
	let engine = TransactionMulti::testing().await;

	// c1
	{
		let mut txn = engine.begin_command().await.unwrap();

		txn.set(&as_key!(0), as_values!(0u64)).unwrap();
		txn.set(&as_key!(u64::MAX), as_values!(u64::MAX)).unwrap();

		txn.set(&as_key!(3), as_values!(31u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(2, engine.version().unwrap());
	}

	// a2, c2
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(12u64)).unwrap();
		txn.set(&as_key!(3), as_values!(32u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(3, engine.version().unwrap());
	}

	// b3
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(13u64)).unwrap();
		txn.set(&as_key!(2), as_values!(23u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(4, engine.version().unwrap());
	}

	// b4 (remove)
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.remove(&as_key!(2)).unwrap();
		txn.commit().unwrap();
		assert_eq!(5, engine.version().unwrap());
	}

	let check_iter = |itr: TransactionRangeIter<'_, _>, expected: &[u64]| {
		let mut i = 0;
		for r in itr {
			assert_eq!(expected[i], from_values!(u64, *r.values()));
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let check_rev_iter = |itr: TransactionRangeRevIter<'_, _>, expected: &[u64]| {
		let mut i = 0;
		for r in itr {
			assert_eq!(expected[i], from_values!(u64, *r.values()));
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let ten_to_one = EncodedKeyRange::start_end(Some(as_key!(10)), Some(as_key!(1)));

	let mut txn = engine.begin_command().await.unwrap();
	let itr = txn.range(ten_to_one.clone()).unwrap();
	check_iter(itr, &[32, 13]);
	let itr = txn.range_rev(ten_to_one.clone()).unwrap();
	check_rev_iter(itr, &[13, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(6));
	let itr = txn.range(ten_to_one.clone()).unwrap();
	let mut count = 2;
	for v in itr {
		if *v.key() == as_key!(1) {
			count -= 1;
		}

		if *v.key() == as_key!(3) {
			count -= 1;
		}
	}
	assert_eq!(0, count);

	let itr = txn.range(ten_to_one.clone()).unwrap();
	let mut count = 2;
	for v in itr {
		if *v.key() == as_key!(1) {
			count -= 1;
		}

		if *v.key() == as_key!(3) {
			count -= 1;
		}
	}
	assert_eq!(0, count);

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let itr = txn.range(ten_to_one.clone()).unwrap();
	check_iter(itr, &[32, 23, 13]);

	let itr = txn.range_rev(ten_to_one.clone()).unwrap();
	check_rev_iter(itr, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let itr = txn.range(ten_to_one.clone()).unwrap();
	check_iter(itr, &[32, 12]);

	let itr = txn.range_rev(ten_to_one.clone()).unwrap();
	check_rev_iter(itr, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let itr = txn.range(ten_to_one.clone()).unwrap();
	check_iter(itr, &[31]);
	let itr = txn.range_rev(ten_to_one.clone()).unwrap();
	check_rev_iter(itr, &[31]);
}
