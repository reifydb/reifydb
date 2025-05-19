// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![deny(clippy::unwrap_used)]
// #![deny(clippy::expect_used)]

pub use engine::{Key, ScanIterator, StorageEngine, Value};
pub use error::Error;
pub use memory::{Memory, MemoryScanIter};
use std::result;

mod engine;
mod error;
mod memory;
pub mod test;

pub type Result<T> = result::Result<T, Error>;
