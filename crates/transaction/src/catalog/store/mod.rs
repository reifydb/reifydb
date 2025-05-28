// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use r#impl::Store;

mod r#impl;

use reifydb_core::expression::Expression;
use reifydb_core::{StoreKind, ValueKind};

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub value: ValueKind,
    pub default: Option<Expression>,
}

pub trait StoreRx {
    fn kind(&self) -> crate::Result<StoreKind>;

    fn get_column(&self, column: &str) -> crate::Result<Column>;

    fn list_columns(&self) -> crate::Result<Vec<Column>>;

    fn get_column_index(&self, column: &str) -> crate::Result<usize>;
}

pub trait StoreTx: StoreRx {}

pub struct NopStore {}

impl StoreRx for NopStore {
    fn kind(&self) -> crate::Result<StoreKind> {
        todo!()
    }

    fn get_column(&self, _column: &str) -> crate::Result<Column> {
        unreachable!()
    }

    fn list_columns(&self) -> crate::Result<Vec<Column>> {
        unreachable!()
    }

    fn get_column_index(&self, _column: &str) -> crate::Result<usize> {
        unreachable!()
    }
}
