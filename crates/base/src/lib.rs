// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

use crate::catalog::Catalog;
pub use core::result;
pub use key::{Key, KeyError};
pub use row::{Row, RowIter, RowIterator};
use std::collections::HashMap;
pub use value::{Value, ValueType};

pub mod catalog;
pub mod expression;
mod key;
mod row;
mod value;

#[derive(Debug)]
pub struct Table {
    pub rows: Vec<Row>,
}

impl Table {
    pub fn scan(&self) -> RowIter {
        Box::new(self.rows.clone().into_iter())
    }
}

#[derive(Debug)]
pub struct Database<C: Catalog> {
    pub catalog: C,
    pub tables: HashMap<String, Table>,
}
