// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::from_row;
use crate::mvcc::transaction::FromRow;
use crate::{as_key, as_row};
use reifydb_transaction::mvcc::transaction::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::iter_rev::TransactionIterRev;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;

#[test]
fn test_versions() {
    let engine = Serializable::testing();

    let k0 = as_key!(0);

    for i in 1..10 {
        let mut txn = engine.begin_command().unwrap();
        txn.set(&k0, as_row!(i)).unwrap();
        txn.commit().unwrap();
        assert_eq!(i + 1, engine.version().unwrap());
    }

    let check_iter = |itr: TransactionIter<'_, _>, i: u64| {
        let mut count = 0;
        for sv in itr {
            assert_eq!(sv.key(), &k0);
            let value = from_row!(u64, sv.row());
            assert_eq!(value, i, "{i} {:?}", value);
            count += 1;
        }
        assert_eq!(1, count) // should only loop once.
    };

    let check_rev_iter = |itr: TransactionIterRev<'_, _>, i: u64| {
        let mut count = 0;
        for sv in itr {
            let value = from_row!(u64, sv.row());
            assert_eq!(value, i, "{i} {:?}", value);
            count += 1;
        }
        assert_eq!(1, count) // should only loop once.
    };

    for idx in 1..10 {
        let mut txn = engine.begin_command().unwrap();
        txn.as_of_version(idx + 1); // Read version at idx.

        let v = idx;
        {
            let sv = txn.get(&k0).unwrap().unwrap();
            assert_eq!(v, from_row!(u64, *sv.row()));
        }

        // Try retrieving the latest version forward and reverse.
        let itr = txn.scan().unwrap();
        check_iter(itr, idx);

        let itr = txn.scan_rev().unwrap();
        check_rev_iter(itr, idx);
    }

    let mut txn = engine.begin_command().unwrap();
    let sv = txn.get(&k0).unwrap().unwrap();
    let val = from_row!(u64, *sv.row());
    assert_eq!(9, val)
}
