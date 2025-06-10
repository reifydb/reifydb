// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use storage::{
    Apply, Contains, Get, Scan, ScanIterator, ScanIteratorRev, ScanRange, ScanRangeIterator,
    ScanRangeIteratorRev, ScanRangeRev, ScanRev, Storage,
};
pub use value::Stored;

pub mod lmdb;
pub mod memory;
pub mod sqlite;
mod storage;
mod value;
