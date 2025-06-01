// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https: //github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
// Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
// http: //www.apache.org/licenses/LICENSE-2.0

use crate::AsyncCowVec;
use crate::FromValue;
use crate::IntoValue;
use crate::keycode;
use crate::{as_key, as_value, from_value};
use MvccError::Transaction;
use TransactionError::Conflict;
use reifydb_persistence::Key;
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::MvccError;
use reifydb_transaction::mvcc::error::TransactionError;
use reifydb_transaction::mvcc::transaction::optimistic::{Optimistic, TransactionTx};

#[test]
fn test_write_skew() {
    // accounts
    let a999: Key = as_key!(999);
    let a888: Key = as_key!(888);

    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

    // Set balance to $100 in each account.
    let mut txn = engine.begin();
    txn.set(a999.clone(), as_value!(100u64)).unwrap();
    txn.set(a888.clone(), as_value!(100u64)).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, engine.version());

    let get_bal = |txn: &mut TransactionTx<Memory>, k: &Key| -> u64 {
        let sv = txn.get(k).unwrap().unwrap();
        let val = sv.value();
        from_value!(u64, val)
    };

    // Start two transactions, each would read both accounts and deduct from one account.
    let mut txn1 = engine.begin();

    let mut sum = get_bal(&mut txn1, &a999);
    sum += get_bal(&mut txn1, &a888);
    assert_eq!(200, sum);
    txn1.set(a999.clone(), as_value!(0)).unwrap(); // Deduct 100 from a999

    // Let's read this back.
    let mut sum = get_bal(&mut txn1, &a999);
    assert_eq!(0, sum);
    sum += get_bal(&mut txn1, &a888);
    assert_eq!(100, sum);
    // Don't commit yet.

    let mut txn2 = engine.begin();

    let mut sum = get_bal(&mut txn2, &a999);
    sum += get_bal(&mut txn2, &a888);
    assert_eq!(200, sum);
    txn2.set(a888.clone(), as_value!(0)).unwrap(); // Deduct 100 from a888

    // Let's read this back.
    let mut sum = get_bal(&mut txn2, &a999);
    assert_eq!(100, sum);
    sum += get_bal(&mut txn2, &a888);
    assert_eq!(100, sum);

    // Commit both now.
    txn1.commit().unwrap();
    let err = txn2.commit().unwrap_err();
    assert_eq!(err, Transaction(Conflict));

    assert_eq!(2, engine.version());
}

// https://wiki.postgresql.org/wiki/SSI#Black_and_White
#[test]
fn test_black_white() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

    // Setup
    let mut txn = engine.begin();
    for i in 1..=10 {
        if i % 2 == 1 {
            txn.set(as_key!(i), as_value!("black".to_string())).unwrap();
        } else {
            txn.set(as_key!(i), as_value!("white".to_string())).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut white = engine.begin();
    let indices = white
        .scan()
        .unwrap()
        .filter_map(|sv| {
            if *sv.value() == as_value!("black".to_string()) {
                Some(sv.key().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for i in indices {
        white.set(i, as_value!("white".to_string())).unwrap();
    }

    let mut black = engine.begin();
    let indices = black
        .scan()
        .unwrap()
        .filter_map(|sv| {
            if *sv.value() == as_value!("white".to_string()) {
                Some(sv.key().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for i in indices {
        black.set(i, as_value!("black".to_string())).unwrap();
    }

    black.commit().unwrap();
    let err = white.commit().unwrap_err();
    assert_eq!(err, Transaction(Conflict));

    let rx = engine.begin_read_only();
    let result: Vec<_> = rx.iter().collect();
    assert_eq!(result.len(), 10);

    result.iter().for_each(|sv| {
        assert_eq!(sv.value, as_value!("black".to_string()));
    })
}

// https://wiki.postgresql.org/wiki/SSI#Overdraft_Protection
#[test]
fn test_overdraft_protection() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

    let key = as_key!("karen");

    // Setup
    let mut txn = engine.begin();
    txn.set(key.clone(), as_value!(1000)).unwrap();
    txn.commit().unwrap();

    // txn1
    let mut txn1 = engine.begin();
    let money = from_value!(i32, *txn1.get(&key).unwrap().unwrap().value());
    txn1.set(key.clone(), as_value!(money - 500)).unwrap();

    // txn2
    let mut txn2 = engine.begin();
    let money = from_value!(i32, *txn2.get(&key).unwrap().unwrap().value());
    txn2.set(key.clone(), as_value!(money - 500)).unwrap();

    txn1.commit().unwrap();
    let err = txn2.commit().unwrap_err();
    assert_eq!(err, Transaction(Conflict));

    let rx = engine.begin_read_only();
    let money = from_value!(i32, *rx.get(&key).unwrap().value());
    assert_eq!(money, 500);
}

// https://wiki.postgresql.org/wiki/SSI#Primary_Colors
#[test]
fn test_primary_colors() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

    // Setup
    let mut txn = engine.begin();
    for i in 1..=9000 {
        if i % 3 == 1 {
            txn.set(as_key!(i), as_value!("red".to_string())).unwrap();
        } else if i % 3 == 2 {
            txn.set(as_key!(i), as_value!("yellow".to_string())).unwrap();
        } else {
            txn.set(as_key!(i), as_value!("blue".to_string())).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut red = engine.begin();
    let indices = red
        .scan()
        .unwrap()
        .filter_map(|sv| {
            if *sv.value() == as_value!("yellow".to_string()) {
                Some(sv.key().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for i in indices {
        red.set(i, as_value!("red".to_string())).unwrap();
    }

    let mut yellow = engine.begin();
    let indices = yellow
        .scan()
        .unwrap()
        .filter_map(|sv| {
            if *sv.value() == as_value!("blue".to_string()) { Some(sv.key().clone()) } else { None }
        })
        .collect::<Vec<_>>();
    for i in indices {
        yellow.set(i, as_value!("yellow".to_string())).unwrap();
    }

    let mut red_two = engine.begin();
    let indices = red_two
        .scan()
        .unwrap()
        .filter_map(|sv| {
            if *sv.value() == as_value!("blue".to_string()) { Some(sv.key().clone()) } else { None }
        })
        .collect::<Vec<_>>();
    for i in indices {
        red_two.set(i, as_value!("red".to_string())).unwrap();
    }

    red.commit().unwrap();
    let err = red_two.commit().unwrap_err();
    assert_eq!(err, Transaction(Conflict));

    let err = yellow.commit().unwrap_err();
    assert_eq!(err, Transaction(Conflict));

    let rx = engine.begin_read_only();
    let result: Vec<_> = rx.iter().collect();
    assert_eq!(result.len(), 9000);

    let mut red_count = 0;
    let mut yellow_count = 0;
    let mut blue_count = 0;

    result.iter().for_each(|sv| {
        let value = from_value!(String, sv.value);
        match value.as_str() {
            "red" => red_count += 1,
            "yellow" => yellow_count += 1,
            "blue" => blue_count += 1,
            _ => unreachable!(),
        }
    });

    assert_eq!(red_count, 6000);
    assert_eq!(blue_count, 3000);
    assert_eq!(yellow_count, 0);
}
