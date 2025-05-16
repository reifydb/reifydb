// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::ValueType;
use base::expression::Expression;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub value: ValueType,
    pub default: Option<Expression>,
}

pub trait StoreRx {
    fn get_column(&self, column: impl AsRef<str>) -> crate::Result<Column>;

    fn list_columns(&self) -> crate::Result<Vec<Column>>;

    fn get_column_index(&self, column: impl AsRef<str>) -> crate::Result<usize>;
}

pub trait StoreTx: StoreRx {}

pub struct NopStore {}

impl StoreRx for NopStore {
    fn get_column(&self, _column: impl AsRef<str>) -> crate::Result<Column> {
        unreachable!()
    }

    fn list_columns(&self) -> crate::Result<Vec<Column>> {
        unreachable!()
    }

    fn get_column_index(&self, _column: impl AsRef<str>) -> crate::Result<usize> {
        unreachable!()
    }
}
