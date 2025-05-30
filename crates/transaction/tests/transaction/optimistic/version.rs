// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::AsyncCowVec;
use crate::FromValue;
use crate::keycode;
use crate::{IntoValue, from_value};
use crate::{as_key, as_value};
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::scan::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::scan::rev_iter::TransactionRevIter;

#[test]
#[cfg(test)]
fn test_versions() {
    let engine: Optimistic = Optimistic::new();

    let k0 = as_key!(0);

    for i in 1..10 {
        let mut txn = engine.begin();
        txn.set(k0.clone(), as_value!(i)).unwrap();
        txn.commit().unwrap();
        assert_eq!(i, engine.version());
    }

    let check_iter = |itr: TransactionIter<'_, BTreeConflict>, i: u64| {
        let mut count = 0;
        for ent in itr {
            assert_eq!(ent.key(), &k0);
            let value = from_value!(u64, ent.value());
            assert_eq!(value, i, "{i} {:?}", value);
            count += 1;
        }
        assert_eq!(1, count) // should only loop once.
    };

    let check_rev_iter = |itr: TransactionRevIter<'_, BTreeConflict>, i: u64| {
        let mut count = 0;
        for ent in itr {
            let value = from_value!(u64, ent.value());
            assert_eq!(value, i, "{i} {:?}", value);
            count += 1;
        }
        assert_eq!(1, count) // should only loop once.
    };

    for idx in 1..10 {
        let mut txn = engine.begin();
        txn.as_of_version(idx); // Read version at idx.

        let v = idx;
        {
            let item = txn.get(&k0).unwrap().unwrap();
            assert_eq!(v, from_value!(u64, *item.value()));
        }

        // Try retrieving the latest version forward and reverse.
        let itr = txn.iter().unwrap();
        check_iter(itr, idx);

        let itr = txn.iter_rev().unwrap();
        check_rev_iter(itr, idx);
    }

    let mut txn = engine.begin();
    let item = txn.get(&k0).unwrap().unwrap();
    let val = from_value!(u64, *item.value());
    assert_eq!(9, val)
}
