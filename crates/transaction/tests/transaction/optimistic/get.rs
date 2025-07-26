// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::transaction::EncodedKey;
use crate::transaction::IntoRow;
use crate::transaction::keycode;
use crate::{as_key, as_row};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_read_after_write() {
    const N: u64 = 100;

    let engine = Optimistic::testing();

    let handles = (0..N)
        .map(|i| {
            let db = engine.clone();
            std::thread::spawn(move || {
                let k = as_key!(i);
                let v = as_row!(i);

                let mut txn = db.begin_tx().unwrap();
                txn.set(&k, v.clone()).unwrap();
                txn.commit().unwrap();

                let txn = db.begin_rx().unwrap();
                let sv = txn.get(&k).unwrap().unwrap();
                assert_eq!(*sv.row(), v);
            })
        })
        .collect::<Vec<_>>();

    handles.into_iter().for_each(|h| {
        h.join().unwrap();
    });
}
