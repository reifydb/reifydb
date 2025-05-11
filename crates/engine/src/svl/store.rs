// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::schema::{Column, StoreName};

pub struct Store {
    pub name: StoreName,
    pub columns: Vec<Column>,
}

impl crate::Store for Store {
    fn column_index(&self, column: impl AsRef<str>) -> crate::Result<usize> {
        let column_name = column.as_ref();
        for (idx, column) in self.columns.iter().enumerate() {
            if &column.name == column_name {
                return Ok(idx);
            }
        }
        // None
        todo!()
    }
}

impl crate::StoreMut for Store {}
