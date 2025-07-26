// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::as_key;
use crate::transaction::EncodedKey;
use crate::transaction::FromRow;
use crate::transaction::IntoRow;
use crate::transaction::keycode;
use crate::{as_row, from_row};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_write() {
    let key = as_key!("foo");

    let engine = Optimistic::testing();
    {
        let mut tx = engine.begin_tx().unwrap();
        assert_eq!(tx.version(), 1);

        tx.set(&key, as_row!("foo1".to_string())).unwrap();
        let value: String = from_row!(String, *tx.get(&key).unwrap().unwrap().row());
        assert_eq!(value.as_str(), "foo1");
        tx.commit().unwrap();
    }

    {
        let rx = engine.begin_rx().unwrap();
        assert_eq!(rx.version(), 2);
        let value: String = from_row!(String, *rx.get(&key).unwrap().unwrap().row());
        assert_eq!(value.as_str(), "foo1");
    }
}

#[test]
fn test_multiple_write() {
    let engine = Optimistic::testing();

    {
        let mut txn = engine.begin_tx().unwrap();
        for i in 0..10 {
            if let Err(e) = txn.set(&as_key!(i), as_row!(i)) {
                panic!("{e}");
            }
        }

        let key = as_key!(8);
        let sv = txn.get(&key).unwrap().unwrap();
        assert!(!sv.is_committed());
        assert_eq!(from_row!(i32, *sv.row()), 8);
        drop(sv);

        assert!(txn.contains_key(&as_key!(8)).unwrap());

        txn.commit().unwrap();
    }

    let k = 8;
    let v = 8;
    let txn = engine.begin_rx().unwrap();
    assert!(txn.contains_key(&as_key!(k)).unwrap());
    let sv = txn.get(&as_key!(k)).unwrap().unwrap();
    assert_eq!(from_row!(i32, *sv.row()), v);
}
