// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{CommitVersion, EncodedKeyRange};

use super::test_multi;
use crate::{as_key, as_values, from_values, multi::transaction::FromValues};

#[test]
fn test_range() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();
	txn.commit().unwrap();

	let four_to_one = EncodedKeyRange::start_end(Some(as_key!(4)), Some(as_key!(1)));

	let txn = engine.begin_query().unwrap();
	let items: Vec<_> = txn.range(four_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.values, as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> = txn.range_rev(four_to_one, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.values, as_values!(expected));
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

	let four_to_one = EncodedKeyRange::start_end(Some(as_key!(4)), Some(as_key!(1)));

	let items: Vec<_> = txn.range(four_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	let items: Vec<_> = txn.range_rev(four_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	txn.commit().unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let seven_to_one = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(1)));

	let items: Vec<_> = txn.range(seven_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=6).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> = txn.range_rev(seven_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=6).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
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

	let seven_to_four = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(4)));

	let items: Vec<_> = txn.range(seven_to_four.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (4..=6).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	let items: Vec<_> = txn.range_rev(seven_to_four.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (4..=6).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 1);
	}

	txn.commit().unwrap();

	let five_to_one = EncodedKeyRange::start_end(Some(as_key!(5)), Some(as_key!(1)));

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let items: Vec<_> = txn.range(five_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=5).rev().zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 2);
	}

	let items: Vec<_> = txn.range_rev(five_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=5).zip(items) {
		assert_eq!(&v.key, &as_key!(expected));
		assert_eq!(&v.values, &as_values!(expected));
		assert_eq!(v.version, 2);
	}
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_range_edge() {
	let engine = test_multi();

	// c1
	{
		let mut txn = engine.begin_command().unwrap();

		txn.set(&as_key!(0), as_values!(0u64)).unwrap();
		txn.set(&as_key!(u64::MAX), as_values!(u64::MAX)).unwrap();

		txn.set(&as_key!(3), as_values!(31u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(2, engine.version().unwrap());
	}

	// a2, c2
	{
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(12u64)).unwrap();
		txn.set(&as_key!(3), as_values!(32u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(3, engine.version().unwrap());
	}

	// b3
	{
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(13u64)).unwrap();
		txn.set(&as_key!(2), as_values!(23u64)).unwrap();
		txn.commit().unwrap();
		assert_eq!(4, engine.version().unwrap());
	}

	// b4 (remove)
	{
		let mut txn = engine.begin_command().unwrap();
		txn.remove(&as_key!(2)).unwrap();
		txn.commit().unwrap();
		assert_eq!(5, engine.version().unwrap());
	}

	let check_iter = |items: Vec<reifydb_core::interface::MultiVersionValues>, expected: &[u64]| {
		let mut i = 0;
		for r in items {
			assert_eq!(expected[i], from_values!(u64, &r.values));
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let check_rev_iter = |items: Vec<reifydb_core::interface::MultiVersionValues>, expected: &[u64]| {
		let mut i = 0;
		for r in items {
			assert_eq!(expected[i], from_values!(u64, &r.values));
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let ten_to_one = EncodedKeyRange::start_end(Some(as_key!(10)), Some(as_key!(1)));

	let mut txn = engine.begin_command().unwrap();
	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 13]);
	let items: Vec<_> = txn.range_rev(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(6));
	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
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

	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
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

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 23, 13]);

	let items: Vec<_> = txn.range_rev(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 12]);

	let items: Vec<_> = txn.range_rev(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let items: Vec<_> = txn.range(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[31]);
	let items: Vec<_> = txn.range_rev(ten_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[31]);
}

/// Regression test for MVCC flickering bug.
///
/// When a key has many versions and batch_size is smaller than the number
/// of versions, the query must still return the newest version.
///
/// Bug scenario (before fix):
/// - Key "foo" has versions 1, 2, 3, ..., 50
/// - Query with batch_size=5 using ASC order
/// - Storage returns oldest versions first
/// - Query returns old value instead of newest
///
/// Fixed behavior:
/// - Query with batch_size=5 using DESC order
/// - Storage returns versions newest-first
/// - Query correctly returns newest version
#[test]
fn test_range_stream_returns_newest_version() {
	let engine = test_multi();

	// Create many versions of the SAME key
	// Each commit creates a new version
	const NUM_VERSIONS: u64 = 50;

	for i in 1..=NUM_VERSIONS {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&as_key!(1), as_values!(i)).unwrap();
		txn.commit().unwrap();
	}

	// Query with small batch_size
	// Before fix: would return an older version
	// After fix: returns newest version
	let txn = engine.begin_query().unwrap();
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 5).collect::<Result<Vec<_>, _>>().unwrap();

	assert_eq!(items.len(), 1);
	let item = &items[0];
	assert_eq!(item.key, as_key!(1));
	// Must be the NEWEST version's value
	assert_eq!(from_values!(u64, &item.values), NUM_VERSIONS);
}

/// Test that streaming works correctly across multiple keys, each with many versions.
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
		txn.commit().unwrap();
	}

	// Query with streaming
	let txn = engine.begin_query().unwrap();
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 200).collect::<Result<Vec<_>, _>>().unwrap();

	// Should have all 5 keys, each with newest version
	// Keys are returned in descending order (5, 4, 3, 2, 1)
	assert_eq!(items.len(), 5);

	for (expected_key, item) in (1..=NUM_KEYS).rev().zip(items.iter()) {
		let expected_value = expected_key * 1000 + VERSIONS_PER_KEY;

		assert_eq!(item.key, as_key!(expected_key));
		assert_eq!(from_values!(u64, &item.values), expected_value);
	}
}
