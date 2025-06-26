// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub mod column;
pub mod column_policy;
mod error;
pub mod key;
pub mod schema;
pub mod sequence;
pub mod table;
pub mod test_utils;
mod row;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Catalog {}
