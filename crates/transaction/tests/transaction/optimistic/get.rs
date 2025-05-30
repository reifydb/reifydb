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
use Bound::{Excluded, Included};
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::scan::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::scan::range::TransactionRange;
use reifydb_transaction::mvcc::transaction::scan::rev_iter::TransactionRevIter;
use reifydb_transaction::mvcc::transaction::scan::rev_range::TransactionRevRange;
use std::ops::Bound;

#[test]
fn test_read_after_write() {
    const N: u64 = 100;

    let engine: Optimistic = Optimistic::new();

    let handles = (0..N)
        .map(|i| {
            let db = engine.clone();
            std::thread::spawn(move || {
                let k = as_key!(i);
                let v = as_value!(i);

                let mut txn = db.begin();
                txn.set(k.clone(), v.clone()).unwrap();
                txn.commit().unwrap();

                let txn = db.begin_read_only();
                let item = txn.get(&k).unwrap();
                assert_eq!(*item.value(), v);
            })
        })
        .collect::<Vec<_>>();

    handles.into_iter().for_each(|h| {
        h.join().unwrap();
    });
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
    let itr = txn.iter().unwrap();
    let itr5 = txn4.iter().unwrap();
    check_iter(itr, &[13, 32]);
    check_iter(itr5, &[13, 24]);

    let itr = txn.iter_rev().unwrap();
    let itr5 = txn4.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 13]);
    check_rev_iter(itr5, &[24, 13]);

    txn.as_of_version(3);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[13, 23, 32]);
    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[12, 32]);
    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[31]);
    let itr = txn.iter_rev().unwrap();
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
    let itr = txn.iter().unwrap();
    check_iter(itr, &[13, 32]);
    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 13]);

    txn.as_of_version(3);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[13, 23, 32]);

    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[12, 32]);

    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.iter().unwrap();
    check_iter(itr, &[31]);
    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, &[31]);
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_range_edge() {
    let engine: Optimistic = Optimistic::new();

    // c1
    {
        let mut txn = engine.begin();

        txn.set(as_key!(0), as_value!(0u64)).unwrap();
        txn.set(as_key!(u64::MAX), as_value!(u64::MAX)).unwrap();

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

    let check_iter = |itr: TransactionRange<'_, _, _>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()));
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let check_rev_iter = |itr: TransactionRevRange<'_, _, _>, expected: &[u64]| {
        let mut i = 0;
        for r in itr {
            assert_eq!(expected[i], from_value!(u64, *r.value()));
            i += 1;
        }
        assert_eq!(expected.len(), i);
    };

    let one_to_ten = (Included(as_key!(1)), Excluded(as_key!(10)));

    let mut txn = engine.begin();
    let itr = txn.range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[13, 32]);
    let itr = txn.range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 13]);

    txn.as_of_version(5);
    let itr = txn.range(one_to_ten.clone()).unwrap();
    let mut count = 2;
    for item in itr {
        dbg!(&item);
        if *item.key() == as_key!(1) {
            count -= 1;
        }

        if *item.key() == as_key!(3) {
            count -= 1;
        }
    }
    assert_eq!(0, count);

    let itr = txn.range(one_to_ten.clone()).unwrap();
    let mut count = 2;
    for item in itr {
        if *item.key() == as_key!(1) {
            count -= 1;
        }

        if *item.key() == as_key!(3) {
            count -= 1;
        }
    }
    assert_eq!(0, count);

    txn.as_of_version(3);
    let itr = txn.range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[13, 23, 32]);

    let itr = txn.range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[12, 32]);

    let itr = txn.range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[31]);
    let itr = txn.range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[31]);
}
