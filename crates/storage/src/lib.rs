// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
use reifydb_persistence::{Key, Value};
use std::result;

mod error;
pub mod memory;

pub type Version = u64;

pub struct StoredValue {
    pub key: Key,
    pub value: Value,
    pub version: Version,
}

pub type Result<T> = result::Result<T, Error>;
