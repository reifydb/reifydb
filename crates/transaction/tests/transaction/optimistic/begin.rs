// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::hook::Hooks;
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;

#[test]
fn test_begin_rx() {
    let engine = Optimistic::new(Memory::new(), Memory::new(), Hooks::default());
    let tx = engine.begin_rx();
    assert_eq!(tx.version(), 0);
}

#[test]
fn test_begin_tx() {
    let engine = Optimistic::new(Memory::new(), Memory::new(), Hooks::default());
    let tx = engine.begin_tx();
    assert_eq!(tx.version(), 0);
}
