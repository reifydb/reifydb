// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::AsyncCowVec;
use crate::IntoValue;
use crate::keycode;
use crate::{as_key, as_value};
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_rollback_same_tx() {
    let engine: Optimistic = Optimistic::new();
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.rollback().unwrap();
    assert!(txn.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_rollback_different_tx() {
    let engine: Optimistic = Optimistic::new();
    let mut txn = engine.begin();
    txn.set(as_key!(1), as_value!(1)).unwrap();
    txn.rollback().unwrap();

    let rx = engine.begin_read_only();
    assert!(rx.get(&as_key!(1)).is_none());
}
