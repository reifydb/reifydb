// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use clock::{LocalClock, LogicalClock};
use reifydb_persistence::{Key, Value};
pub use storage::{
    Apply, Contains, Get, Scan, ScanIterator, ScanIteratorRev, ScanRange, ScanRangeIterator,
    ScanRangeIteratorRev, ScanRangeRev, ScanRev, Storage,
};

mod clock;
pub mod memory;
mod storage;

pub type Version = u64;

pub struct StoredValue {
    pub key: Key,
    pub value: Value,
    pub version: Version,
}
