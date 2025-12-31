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
use reifydb_transaction::multi::TransactionMulti;

use crate::{as_key, as_values, from_values, multi::transaction::FromValues};

#[tokio::test]
async fn test_iter() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();
	txn.commit().await.unwrap();

	let txn = engine.begin_query().await.unwrap();
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();

	for (expected, tv) in (1..=3).rev().zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
	}

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=3).zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
	}
}

#[tokio::test]
async fn test_iter2() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=3).rev().zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=3).zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}
	txn.commit().await.unwrap();

	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=6).rev().zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=6).zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}
}

#[tokio::test]
async fn test_iter3() {
	let engine = TransactionMulti::testing().await;
	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(4), as_values!(4)).unwrap();
	txn.set(&as_key!(5), as_values!(5)).unwrap();
	txn.set(&as_key!(6), as_values!(6)).unwrap();

	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (4..=6).rev().zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (4..=6).zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 1);
	}

	txn.commit().await.unwrap();

	let mut txn = engine.begin_command().await.unwrap();
	txn.set(&as_key!(1), as_values!(1)).unwrap();
	txn.set(&as_key!(2), as_values!(2)).unwrap();
	txn.set(&as_key!(3), as_values!(3)).unwrap();

	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=6).rev().zip(batch.items) {
		assert_eq!(tv.key, as_key!(expected));
		assert_eq!(tv.values, as_values!(expected));
		assert_eq!(tv.version, 2);
	}

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	for (expected, tv) in (1..=6).zip(batch.items) {
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
#[tokio::test]
async fn test_iter_edge_case() {
	let engine = TransactionMulti::testing().await;

	// c1
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(3), as_values!(31u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(2, engine.version().await.unwrap());
	}

	// a2, c2
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(12u64)).unwrap();
		txn.set(&as_key!(3), as_values!(32u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(3, engine.version().await.unwrap());
	}

	// b3
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(13u64)).unwrap();
		txn.set(&as_key!(2), as_values!(23u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(4, engine.version().await.unwrap());
	}

	// b4, c4(remove) (uncommitted)
	let mut txn4 = engine.begin_command().await.unwrap();
	txn4.set(&as_key!(2), as_values!(24u64)).unwrap();
	txn4.remove(&as_key!(3)).unwrap();
	assert_eq!(4, engine.version().await.unwrap());

	// b4 (remove)
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.remove(&as_key!(2)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(5, engine.version().await.unwrap());
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

	let mut txn = engine.begin_command().await.unwrap();
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 13]);
	let batch5 = txn4.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch5.items, &[24, 13]);

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[13, 32]);
	let batch5 = txn4.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch5.items, &[13, 24]);

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 23, 13]);
	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 12]);
	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[31]);
	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[31]);
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[tokio::test]
async fn test_iter_edge_case2() {
	let engine = TransactionMulti::testing().await;

	// c1
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(3), as_values!(31u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(2, engine.version().await.unwrap());
	}

	// a2, c2
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(12u64)).unwrap();
		txn.set(&as_key!(3), as_values!(32u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(3, engine.version().await.unwrap());
	}

	// b3
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&as_key!(1), as_values!(13u64)).unwrap();
		txn.set(&as_key!(2), as_values!(23u64)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(4, engine.version().await.unwrap());
	}

	// b4 (remove)
	{
		let mut txn = engine.begin_command().await.unwrap();
		txn.remove(&as_key!(2)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(5, engine.version().await.unwrap());
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

	let mut txn = engine.begin_command().await.unwrap();
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 13]);
	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[13, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(4));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 23, 13]);

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[13, 23, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(3));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[32, 12]);

	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[12, 32]);

	txn.read_as_of_version_exclusive(CommitVersion(2));
	let batch = txn.range(EncodedKeyRange::all()).await.unwrap();
	check_iter(batch.items, &[31]);
	let batch = txn.range_rev(EncodedKeyRange::all()).await.unwrap();
	check_rev_iter(batch.items, &[31]);
}
