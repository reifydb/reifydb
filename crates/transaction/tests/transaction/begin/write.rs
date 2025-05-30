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
use crate::IntoValue;
use crate::bincode;
use crate::into_key;
use crate::{AsyncCowVec, from_value, into_value};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_write() {
    let key = into_key!("foo");

    let engine: Optimistic = Optimistic::new();
    {
        let mut tx = engine.begin();
        assert_eq!(tx.version(), 0);

        tx.set(key.clone(), into_value!("foo1".to_string())).unwrap();
        let value: String = from_value!(String, *tx.get(&key).unwrap().unwrap().value());
        assert_eq!(value.as_str(), "foo1");
        tx.commit().unwrap();
    }

    {
        let rx = engine.begin_read_only();
        assert_eq!(rx.version(), 1);
        let value: String = from_value!(String, *rx.get(&key).unwrap().value());
        assert_eq!(value.as_str(), "foo1");
    }
}

#[test]
fn test_multiple_write() {
    let engine: Optimistic = Optimistic::new();

    {
        let mut txn = engine.begin();
        for i in 0..10 {
            if let Err(e) = txn.set(into_key!(i), into_value!(i)) {
                panic!("{e}");
            }
        }

        let key = into_key!(8);
        let item = txn.get(&key).unwrap().unwrap();
        assert!(!item.is_committed());
        assert_eq!(from_value!(i32, *item.value()), 8);
        drop(item);

        assert!(txn.contains_key(&into_key!(8)).unwrap());

        txn.commit().unwrap();
    }

    let k = 8;
    let v = 8;
    let txn = engine.begin_read_only();
    assert!(txn.contains_key(&into_key!(k)));
    let item = txn.get(&into_key!(k)).unwrap();
    assert_eq!(from_value!(i32, *item.value()), v);
}
