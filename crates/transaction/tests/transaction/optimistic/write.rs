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
use crate::transaction::AsyncCowVec;
use crate::transaction::FromRow;
use crate::transaction::IntoRow;
use crate::transaction::keycode;
use crate::{as_row, from_row};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_write() {
    let key = as_key!("foo");

    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());
    {
        let mut tx = engine.begin();
        assert_eq!(tx.version(), 0);

        tx.set(key.clone(), as_row!("foo1".to_string())).unwrap();
        let value: String = from_row!(String, *tx.get(&key).unwrap().unwrap().row());
        assert_eq!(value.as_str(), "foo1");
        tx.commit().unwrap();
    }

    {
        let rx = engine.begin_read_only();
        assert_eq!(rx.version(), 1);
        let value: String = from_row!(String, *rx.get(&key).unwrap().row());
        assert_eq!(value.as_str(), "foo1");
    }
}

#[test]
fn test_multiple_write() {
    let engine: Optimistic<Memory> = Optimistic::new(Memory::new());

    {
        let mut txn = engine.begin();
        for i in 0..10 {
            if let Err(e) = txn.set(as_key!(i), as_row!(i)) {
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
    let txn = engine.begin_read_only();
    assert!(txn.contains_key(&as_key!(k)));
    let sv = txn.get(&as_key!(k)).unwrap();
    assert_eq!(from_row!(i32, *sv.row()), v);
}
