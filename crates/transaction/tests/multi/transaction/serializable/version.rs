// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::CommitVersion;
use reifydb_transaction::multi::transaction::{
	scan::TransactionScanIter, scan_rev::TransactionScanRevIter, serializable::SerializableTransaction,
};

use crate::{as_key, as_values, from_values, multi::transaction::FromValues};

#[test]
fn test_versions() {
	let engine = SerializableTransaction::testing();

	let k0 = as_key!(0);

	for i in 1..10 {
		let mut txn = engine.begin_command().unwrap();
		txn.set(&k0, as_values!(i)).unwrap();
		txn.commit().unwrap();
		assert_eq!(i + 1, engine.version().unwrap());
	}

	let check_iter = |itr: TransactionScanIter<'_, _>, i: i32| {
		let mut count = 0;
		for sv in itr {
			assert_eq!(sv.key(), &k0);
			let value = from_values!(i32, sv.values());
			assert_eq!(value, i, "{i} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count) // should only loop once.
	};

	let check_rev_iter = |itr: TransactionScanRevIter<'_, _>, i: i32| {
		let mut count = 0;
		for sv in itr {
			let value = from_values!(i32, sv.values());
			assert_eq!(value, i, "{i} {:?}", value);
			count += 1;
		}
		assert_eq!(1, count) // should only loop once.
	};

	for idx in 1i32..10 {
		let mut txn = engine.begin_command().unwrap();
		txn.read_as_of_version_exclusive(CommitVersion(idx as u64 + 1)); // Read version at idx.

		let v = idx;
		{
			let tv = txn.get(&k0).unwrap().unwrap();
			assert_eq!(v, from_values!(i32, *tv.values()));
		}

		// Try retrieving the latest version forward and reverse.
		let itr = txn.scan().unwrap();
		check_iter(itr, idx);

		let itr = txn.scan_rev().unwrap();
		check_rev_iter(itr, idx);
	}

	let mut txn = engine.begin_command().unwrap();
	let sv = txn.get(&k0).unwrap().unwrap();
	let val = from_values!(i32, *sv.values());
	assert_eq!(9, val)
}
