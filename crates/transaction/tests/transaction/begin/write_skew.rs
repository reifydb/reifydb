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
use crate::bincode;
use crate::FromValue;
use crate::IntoValue;
use crate::{from_value, into_key, into_value};
use MvccError::Transaction;
use TransactionError::Conflict;
use reifydb_transaction::Key;
use reifydb_transaction::mvcc::MvccError;
use reifydb_transaction::mvcc::error::TransactionError;
use reifydb_transaction::mvcc::transaction::optimistic::{Optimistic, TransactionTx};
use std::ops::Deref;

#[test]
fn test_txn_write_skew() {
    // accounts
    let a999: Key = into_key!(999);
    let a888: Key = into_key!(888);

    let engine: Optimistic = Optimistic::new();

    // Set balance to $100 in each account.
    let mut txn = engine.begin();
    txn.set(a999.clone(), into_value!(100u64)).unwrap();
    txn.set(a888.clone(), into_value!(100u64)).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, engine.version());

    let get_bal = |txn: &mut TransactionTx, k: &Key| -> u64 {
        let item = txn.get(k).unwrap().unwrap();
        let val = item.value().deref().clone();
        from_value!(u64, val)
    };

    // Start two transactions, each would read both accounts and deduct from one account.
    let mut txn1 = engine.begin();

    let mut sum = get_bal(&mut txn1, &a999);
    sum += get_bal(&mut txn1, &a888);
    assert_eq!(200, sum);
    txn1.set(a999.clone(), into_value!(0)).unwrap(); // Deduct 100 from a999

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
    txn2.set(a888.clone(), into_value!(0)).unwrap(); // Deduct 100 from a888

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
