// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use row::{Row, RowIter, RowIterator};
pub use value::{Value, ValueType};
pub use core::result;

mod row;
mod value;
