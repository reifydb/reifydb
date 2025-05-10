// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::schema::column::Columns;

#[derive(Debug)]
pub struct TableName(String);

impl TableName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: TableName,
    pub columns: Columns,
}

impl Table {
    pub fn column_index(&self, column: impl AsRef<str>) -> Option<usize> {
        let target = column.as_ref();
        for (idx, column) in self.columns.into_iter().enumerate() {
            if column.name == *target {
                return Some(idx);
            }
        }
        None
    }
}
