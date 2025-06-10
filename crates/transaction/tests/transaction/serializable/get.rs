// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::transaction::AsyncCowVec;
use crate::transaction::IntoBytes;
use crate::transaction::keycode;
use crate::{as_bytes, as_key};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;

#[test]
fn test_read_after_write() {
    const N: u64 = 100;

    let engine: Serializable<Memory> = Serializable::new(Memory::new());

    let handles = (0..N)
        .map(|i| {
            let db = engine.clone();
            std::thread::spawn(move || {
                let k = as_key!(i);
                let v = as_bytes!(i);

                let mut txn = db.begin();
                txn.set(k.clone(), v.clone()).unwrap();
                txn.commit().unwrap();

                let txn = db.begin_read_only();
                let sv = txn.get(&k).unwrap();
                assert_eq!(*sv.bytes(), v);
            })
        })
        .collect::<Vec<_>>();

    handles.into_iter().for_each(|h| {
        h.join().unwrap();
    });
}
