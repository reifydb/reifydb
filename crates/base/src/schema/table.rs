// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::schema::Column;

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
    pub columns: Vec<Column>,
}
