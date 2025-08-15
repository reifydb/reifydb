// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_core::Result;

pub mod row;
pub mod schema;
pub mod sequence;
pub mod table;
pub mod table_column;
pub mod table_column_policy;
pub mod test_utils;
pub mod view;
pub mod view_column;

pub struct Catalog {}
