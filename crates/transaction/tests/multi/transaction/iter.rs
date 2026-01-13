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
fn test_iter() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();
	txn.commit().unwrap();

	let txn = engine.begin_query().unwrap();
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();

	for (expected, tv) in (1..=3).rev().zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
	}

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=3).zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
	}
}

#[test]
fn test_iter2() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=3).rev().zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=3).zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}
	txn.commit().unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=6).rev().zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=6).zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}
}

#[test]
fn test_iter3() {
	let engine = test_multi();
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (4..=6).rev().zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (4..=6).zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	txn.commit().unwrap();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=6).rev().zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, tv) in (1..=6).zip(items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}
}

/// a3, a2, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=4(Uncommitted) -> a3, b4
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_iter_edge_case() {
	let engine = test_multi();

	// c1
	{
		let mut txn = engine.begin_command().unwrap();
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

	// b4, c4(remove) (uncommitted)
	let mut txn4 = engine.begin_command().unwrap();
	txn4.set(&as_key!(2), as_values!(24u64)).unwrap();
	txn4.remove(&as_key!(3)).unwrap();
	assert_eq!(4, engine.version().unwrap());

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
			assert_eq!(expected[i], from_values!(u64, &r.values), "read_vs={}", r.version);
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let check_rev_iter = |items: Vec<reifydb_core::interface::MultiVersionValues>, expected: &[u64]| {
		let mut i = 0;
		for r in items {
			assert_eq!(expected[i], from_values!(u64, &r.values), "read_vs={}", r.version);
			i += 1;
		}
		assert_eq!(expected.len(), i);
	};

	let mut txn = engine.begin_command().unwrap();
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 13]);
	let items: Vec<_> = txn4.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[24, 13]);

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 32]);
	let items: Vec<_> = txn4.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 24]);

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 23, 13]);
	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 12]);
	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[31]);
	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[31]);
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_iter_edge_case2() {
	let engine = test_multi();

	// c1
	{
		let mut txn = engine.begin_command().unwrap();
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

	let mut txn = engine.begin_command().unwrap();
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 13]);
	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 23, 13]);

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[32, 12]);

	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_iter(items, &[31]);
	let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	check_rev_iter(items, &[31]);
}
