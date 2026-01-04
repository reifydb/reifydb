// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use futures_util::TryStreamExt;
use reifydb_core::{CommitVersion, EncodedKeyRange};
use reifydb_transaction::multi::TransactionMulti;

use crate::{as_key, as_values, from_values, multi::transaction::FromValues};

#[tokio::test]
async fn test_versions() {
	let engine = TransactionMulti::testing().await;

	let k0 = as_key!(0);

	for i in 1..10 {
		let mut txn = engine.begin_command().await.unwrap();
		txn.set(&k0, as_values!(i)).unwrap();
		txn.commit().await.unwrap();
		assert_eq!(i + 1, engine.version().await.unwrap());
	}

	for idx in 1i32..10 {
		let mut txn = engine.begin_command().await.unwrap();
		txn.read_as_of_version_exclusive(CommitVersion(idx as u64 + 1)); // Read version at idx.

		let v = idx;
		{
			let tv = txn.get(&k0).await.unwrap().unwrap();
			assert_eq!(v, from_values!(i32, tv.values()));
		}

		// Try retrieving the latest version forward and reverse.
		let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).try_collect().await.unwrap();
		let mut count = 0;
		for sv in items {
			assert_eq!(&sv.key, &k0);
			let value = from_values!(i32, &sv.values);
			assert_eq!(value, idx, "{idx} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count); // should only loop once.

		let items: Vec<_> = txn.range_rev(EncodedKeyRange::all(), 1024).try_collect().await.unwrap();
		let mut count = 0;
		for sv in items {
			let value = from_values!(i32, &sv.values);
			assert_eq!(value, idx, "{idx} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count); // should only loop once.
	}

	let mut txn = engine.begin_command().await.unwrap();
	let sv = txn.get(&k0).await.unwrap().unwrap();
	let val = from_values!(i32, sv.values());
	assert_eq!(9, val)
}
