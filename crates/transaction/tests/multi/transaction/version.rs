// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::common::CommitVersion;
use reifydb_transaction::multi::RangeScope;

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

#[test]
fn test_versions() {
	let engine = test_multi();

	let k0 = as_key!(0);

	for i in 1..10 {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&k0, as_values!(i)).unwrap();
		txn.commit(vec![]).unwrap();
		assert_eq!(i + 1, engine.version().unwrap());
	}

	for idx in 1i32..10 {
		let mut txn = engine.begin_command().unwrap();
		txn.read_as_of_version_inclusive(CommitVersion(idx as u64 + 1)).unwrap(); // Read the write committed at version idx + 1.

		let v = idx;
		{
			let tv = txn.get(&k0).unwrap().unwrap();
			assert_eq!(v, from_row!(i32, tv.row()));
		}

		// Try retrieving the latest version forward and reverse.
		let items: Vec<_> = txn
			.range(EncodedKeyRange::all(), RangeScope::All, 1024)
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		let mut count = 0;
		for sv in items {
			assert_eq!(&sv.key, &k0);
			let value = from_row!(i32, &sv.row);
			assert_eq!(value, idx, "{idx} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count); // should only loop once.

		let items: Vec<_> = txn
			.range_rev(EncodedKeyRange::all(), RangeScope::All, 1024)
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		let mut count = 0;
		for sv in items {
			let value = from_row!(i32, &sv.row);
			assert_eq!(value, idx, "{idx} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count); // should only loop once.
	}

	let mut txn = engine.begin_command().unwrap();
	let sv = txn.get(&k0).unwrap().unwrap();
	let val = from_row!(i32, sv.row());
	assert_eq!(9, val)
}

#[test]
fn test_as_of_version_bounds() {
	// Pins the as-of contract: inclusive(v) sees writes committed at exactly v,
	// exclusive(v) does not. The first commit lands at version 2 (version 1 is
	// the empty genesis snapshot), so the boundary is observable directly.
	let engine = test_multi();
	let k0 = as_key!(0);

	let mut txn = engine.begin_command().unwrap();
	txn.set(&k0, as_values!(1)).unwrap();
	txn.commit(vec![]).unwrap();
	let committed_at = engine.version().unwrap();
	assert_eq!(CommitVersion(2), committed_at);

	let mut rx = engine.begin_query().unwrap();
	rx.read_as_of_version_inclusive(committed_at);
	assert_eq!(1, from_row!(i32, rx.get(&k0).unwrap().unwrap().row()));

	let mut rx = engine.begin_query().unwrap();
	rx.read_as_of_version_inclusive(CommitVersion(committed_at.0 - 1));
	assert!(rx.get(&k0).unwrap().is_none());

	let mut rx = engine.begin_query().unwrap();
	rx.read_as_of_version_exclusive(committed_at);
	assert!(rx.get(&k0).unwrap().is_none());

	let mut rx = engine.begin_query().unwrap();
	rx.read_as_of_version_exclusive(CommitVersion(committed_at.0 + 1));
	assert_eq!(1, from_row!(i32, rx.get(&k0).unwrap().unwrap().row()));

	let mut wx = engine.begin_command().unwrap();
	wx.read_as_of_version_inclusive(committed_at).unwrap();
	assert_eq!(1, from_row!(i32, wx.get(&k0).unwrap().unwrap().row()));

	let mut wx = engine.begin_command().unwrap();
	wx.read_as_of_version_exclusive(committed_at);
	assert!(wx.get(&k0).unwrap().is_none());
}
