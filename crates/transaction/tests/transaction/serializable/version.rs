// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::from_value;
use crate::transaction::AsyncCowVec;
use crate::transaction::FromValue;
use crate::transaction::IntoValue;
use crate::transaction::keycode;
use crate::{as_key, as_value};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::iter_rev::TransactionIterRev;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;

#[test]
#[cfg(test)]
fn test_versions() {
    let engine: Serializable<Memory> = Serializable::new(Memory::new());

    let k0 = as_key!(0);

    for i in 1..10 {
        let mut txn = engine.begin();
        txn.set(k0.clone(), as_value!(i)).unwrap();
        txn.commit().unwrap();
        assert_eq!(i, engine.version());
    }

    let check_iter = |itr: TransactionIter<'_, _, BTreeConflict>, i: u64| {
        let mut count = 0;
        for sv in itr {
            assert_eq!(sv.key(), &k0);
            let value = from_value!(u64, sv.value());
            assert_eq!(value, i, "{i} {:?}", value);
            count += 1;
        }
        assert_eq!(1, count) // should only loop once.
    };

    let check_rev_iter = |itr: TransactionIterRev<'_, _, BTreeConflict>, i: u64| {
        let mut count = 0;
        for sv in itr {
            let value = from_value!(u64, sv.value());
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
            let sv = txn.get(&k0).unwrap().unwrap();
            assert_eq!(v, from_value!(u64, *sv.value()));
        }

        // Try retrieving the latest version forward and reverse.
        let itr = txn.scan().unwrap();
        check_iter(itr, idx);

        let itr = txn.scan_rev().unwrap();
        check_rev_iter(itr, idx);
    }

    let mut txn = engine.begin();
    let sv = txn.get(&k0).unwrap().unwrap();
    let val = from_value!(u64, *sv.value());
    assert_eq!(9, val)
}
