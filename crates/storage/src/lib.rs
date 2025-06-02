// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use action::Action;
pub use clock::{LocalClock, LogicalClock};
pub use key::KeyRange;
use reifydb_core::AsyncCowVec;
pub use storage::{
    Apply, Contains, Get, Scan, ScanIterator, ScanIteratorRev, ScanRange, ScanRangeIterator,
    ScanRangeIteratorRev, ScanRangeRev, ScanRev, Storage,
};

mod action;
mod clock;
mod key;
pub mod lmdb;
pub mod memory;
mod storage;

pub type Version = u64;

pub type Key = AsyncCowVec<u8>;
pub type Value = AsyncCowVec<u8>;

pub struct StoredValue {
    pub key: Key,
    pub value: Value,
    pub version: Version,
}
