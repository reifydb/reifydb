// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::FromValue;
use crate::as_key;
use crate::keycode;
use crate::{AsyncCowVec, as_value};
use crate::{IntoValue, from_value};
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::iter_rev::TransactionRevIter;

#[test]
fn test_iter() {
    let engine: Optimistic = Optimistic::new();
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin_read_only();
    let iter = txn.iter();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key, as_key!(count));
        assert_eq!(sv.value, as_value!(count));
    }
    assert_eq!(count, 3);

    let iter = txn.iter_rev();
    let mut count = 3;
    for sv in iter {
        assert_eq!(sv.key, as_key!(count));
        assert_eq!(sv.value, as_value!(count));
        count -= 1;
    }
    assert_eq!(count, 0);
}

#[test]
fn test_iter2() {
    let engine: Optimistic = Optimistic::new();
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();

    let iter = txn.scan().unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
    }
    assert_eq!(count, 3);

    let iter = txn.scan_rev().unwrap();
    let mut count = 3;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        count -= 1;
    }

    assert_eq!(count, 0);

    txn.commit().unwrap();

    let mut txn = engine.begin();
    txn.set(as_key!(4), as_value!(4)).unwrap();
    txn.set(as_key!(5), as_value!(5)).unwrap();
    txn.set(as_key!(6), as_value!(6)).unwrap();

    let iter = txn.scan().unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 1);
    }
    assert_eq!(count, 6);

    let iter = txn.scan_rev().unwrap();
    let mut count = 6;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 1);
        count -= 1;
    }
}

#[test]
fn test_iter3() {
    let engine: Optimistic = Optimistic::new();
    let mut txn = engine.begin();
    txn.set(as_key!(4), as_value!(4)).unwrap();
    txn.set(as_key!(5), as_value!(5)).unwrap();
    txn.set(as_key!(6), as_value!(6)).unwrap();

    let iter = txn.scan().unwrap();
    let mut count = 3;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
    }
    assert_eq!(count, 6);

    let iter = txn.scan_rev().unwrap();
    let mut count = 6;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
        count -= 1;
    }
    assert_eq!(count, 3);

    txn.commit().unwrap();

    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();

    let iter = txn.scan().unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 1);
    }
    assert_eq!(count, 6);

    let iter = txn.scan_rev().unwrap();
    let mut count = 6;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 1);
        count -= 1;
    }
    assert_eq!(count, 0);
}

/// a3, a2, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=4(Uncommitted) -> a3, b4
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_iter_edge_case() {
    let engine: Optimistic = Optimistic::new();

    // c1
    {
        let mut txn = engine.begin();
        txn.set(as_key!(3), as_value!(31u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(1, engine.version());
    }

    // a2, c2
    {
        let mut txn = engine.begin();
        txn.set(as_key!(1), as_value!(12u64)).unwrap();
        txn.set(as_key!(3), as_value!(32u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(2, engine.version());
    }

    // b3
    {
        let mut txn = engine.begin();
        txn.set(as_key!(1), as_value!(13u64)).unwrap();
        txn.set(as_key!(2), as_value!(23u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(3, engine.version());
    }

    // b4, c4(remove) (uncommitted)
    let mut txn4 = engine.begin();
    txn4.set(as_key!(2), as_value!(24u64)).unwrap();
    txn4.remove(as_key!(3)).unwrap();
    assert_eq!(3, engine.version());

    // b4 (remove)
    {
        let mut txn = engine.begin();
        txn.remove(as_key!(2)).unwrap();
        txn.commit().unwrap();
        assert_eq!(4, engine.version());
    }

    let check_iter = |itr: TransactionIter<'_, BTreeConflict>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()), "read_vs={}", r.version());
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let check_rev_iter = |itr: TransactionRevIter<'_, BTreeConflict>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()), "read_vs={}", r.version());
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let mut txn = engine.begin();
    let itr = txn.scan().unwrap();
    let itr5 = txn4.scan().unwrap();
    check_iter(itr, &[13, 32]);
    check_iter(itr5, &[13, 24]);

    let itr = txn.scan_rev().unwrap();
    let itr5 = txn4.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 13]);
    check_rev_iter(itr5, &[24, 13]);

    txn.as_of_version(3);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[13, 23, 32]);
    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[12, 32]);
    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[31]);
    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[31]);
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_iter_edge_case2() {
    let engine: Optimistic = Optimistic::new();

    // c1
    {
        let mut txn = engine.begin();
        txn.set(as_key!(3), as_value!(31u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(1, engine.version());
    }

    // a2, c2
    {
        let mut txn = engine.begin();
        txn.set(as_key!(1), as_value!(12u64)).unwrap();
        txn.set(as_key!(3), as_value!(32u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(2, engine.version());
    }

    // b3
    {
        let mut txn = engine.begin();
        txn.set(as_key!(1), as_value!(13u64)).unwrap();
        txn.set(as_key!(2), as_value!(23u64)).unwrap();
        txn.commit().unwrap();
        assert_eq!(3, engine.version());
    }

    // b4 (remove)
    {
        let mut txn = engine.begin();
        txn.remove(as_key!(2)).unwrap();
        txn.commit().unwrap();
        assert_eq!(4, engine.version());
    }

    let check_iter = |itr: TransactionIter<'_, BTreeConflict>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()));
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let check_rev_iter = |itr: TransactionRevIter<'_, BTreeConflict>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()));
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let mut txn = engine.begin();
    let itr = txn.scan().unwrap();
    check_iter(itr, &[13, 32]);
    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 13]);

    txn.as_of_version(3);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[13, 23, 32]);

    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[12, 32]);

    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.scan().unwrap();
    check_iter(itr, &[31]);
    let itr = txn.scan_rev().unwrap();
    check_rev_iter(itr, &[31]);
}
