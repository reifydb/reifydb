// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::as_key;
use crate::keycode;
use crate::FromValue;
use crate::{as_value, AsyncCowVec};
use crate::{from_value, IntoValue};
use reifydb_storage::KeyRange;
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::mvcc::transaction::range::TransactionRange;
use reifydb_transaction::mvcc::transaction::range_rev::TransactionRevRange;
use std::ops::Bound;

#[test]
fn test_range() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();
    txn.commit().unwrap();

    let one_to_four = KeyRange::start_end(Some(as_key!(1)), Some(as_key!(4)));

    let txn = engine.begin_read_only();
    let iter = txn.scan_range(one_to_four.clone());
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key, as_key!(count));
        assert_eq!(sv.value, as_value!(count));
        assert_eq!(sv.version, 1);
    }
    assert_eq!(count, 3);

    let iter = txn.scan_range_rev(one_to_four);
    let mut count = 3;
    for sv in iter {
        assert_eq!(sv.key, as_key!(count));
        assert_eq!(sv.value, as_value!(count));
        assert_eq!(sv.version, 1);
        count -= 1;
    }
    assert_eq!(count, 0);
}

#[test]
fn test_range2() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();

    let one_to_four = KeyRange::start_end(Some(as_key!(1)), Some(as_key!(4)));

    let iter = txn.scan_range(one_to_four.clone()).unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        let sv = sv.clone();
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
    }
    assert_eq!(count, 3);

    let iter = txn.scan_range_rev(one_to_four.clone()).unwrap();
    let mut count = 3;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
        count -= 1;
    }
    assert_eq!(count, 0);

    txn.commit().unwrap();

    let mut txn = engine.begin();
    txn.set(as_key!(4), as_value!(4)).unwrap();
    txn.set(as_key!(5), as_value!(5)).unwrap();
    txn.set(as_key!(6), as_value!(6)).unwrap();

    let one_to_five = KeyRange::start_end(Some(as_key!(1)), Some(as_key!(5)));

    let iter = txn.scan_range(one_to_five.clone()).unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
    }
    assert_eq!(count, 4);

    let iter = txn.scan_range_rev(one_to_five.clone()).unwrap();
    let mut count = 4;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        count -= 1;
    }
    assert_eq!(count, 0);
}

#[test]
fn test_range3() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());
    let mut txn = engine.begin();
    txn.set(as_key!(4), as_value!(4)).unwrap();
    txn.set(as_key!(5), as_value!(5)).unwrap();
    txn.set(as_key!(6), as_value!(6)).unwrap();

    let four_to_seven = KeyRange::start_end(Some(as_key!(4)), Some(as_key!(7)));

    let iter = txn.scan_range(four_to_seven.clone()).unwrap();
    let mut count = 3;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
    }
    assert_eq!(count, 6);

    let iter = txn.scan_range_rev(four_to_seven.clone()).unwrap();
    let mut count = 6;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        assert_eq!(sv.version(), 0);
        count -= 1;
    }
    assert_eq!(count, 3);

    txn.commit().unwrap();

    let one_to_five = KeyRange::start_end(Some(as_key!(1)), Some(as_key!(5)));

    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.set(as_key!(2), as_value!(2)).unwrap();
    txn.set(as_key!(3), as_value!(3)).unwrap();

    let iter = txn.scan_range(one_to_five.clone()).unwrap();
    let mut count = 0;
    for sv in iter {
        count += 1;
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
    }
    assert_eq!(count, 4);

    let iter = txn.scan_range_rev(one_to_five.clone()).unwrap();
    let mut count = 4;
    for sv in iter {
        assert_eq!(sv.key(), &as_key!(count));
        assert_eq!(sv.value(), &as_value!(count));
        count -= 1;
    }
    assert_eq!(count, 0);
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn test_range_edge() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

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

    let one_to_ten = KeyRange::start_end(Some(as_key!(1)), Some(as_key!(10)));

    let mut txn = engine.begin();
    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[13, 32]);
    let itr = txn.scan_range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 13]);

    txn.as_of_version(5);
    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    let mut count = 2;
    for sv in itr {
        dbg!(&sv);
        if *sv.key() == as_key!(1) {
            count -= 1;
        }

        if *sv.key() == as_key!(3) {
            count -= 1;
        }
    }
    assert_eq!(0, count);

    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    let mut count = 2;
    for sv in itr {
        if *sv.key() == as_key!(1) {
            count -= 1;
        }

        if *sv.key() == as_key!(3) {
            count -= 1;
        }
    }
    assert_eq!(0, count);

    txn.as_of_version(3);
    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[13, 23, 32]);

    let itr = txn.scan_range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 23, 13]);

    txn.as_of_version(2);
    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[12, 32]);

    let itr = txn.scan_range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[32, 12]);

    txn.as_of_version(1);
    let itr = txn.scan_range(one_to_ten.clone()).unwrap();
    check_iter(itr, &[31]);
    let itr = txn.scan_range_rev(one_to_ten.clone()).unwrap();
    check_rev_iter(itr, &[31]);
}
