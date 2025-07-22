// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use reifydb_core::Result;


pub mod column;
pub mod column_policy;
pub mod key;
pub mod row;
pub mod schema;
pub mod sequence;
pub mod table;
pub mod test_utils;


pub struct Catalog {}
