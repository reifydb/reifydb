// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #[derive(Debug)]
// pub struct Column {
//     pub name: ColumnName,
//     pub value: ValueType,
//     pub default: Option<Expression>,
// }

use crate::catalog::Column;

pub struct Store {
    pub name: String,
    pub columns: Vec<Column>,
}

impl crate::StoreRx for Store {
    fn get_column(&self, column: &str) -> crate::Result<Column> {
        let column_name = column;
        for (idx, column) in self.columns.iter().enumerate() {
            if &column.name == column_name {
                return Ok(column.clone());
            }
        }
        // None
        todo!()
    }

    fn list_columns(&self) -> crate::Result<Vec<Column>> {
        Ok(self.columns.clone())
    }

    fn get_column_index(&self, column: &str) -> crate::Result<usize> {
        let column_name = column;
        for (idx, column) in self.columns.iter().enumerate() {
            if &column.name == column_name {
                return Ok(idx);
            }
        }
        // None
        todo!()
    }
}

impl crate::StoreTx for Store {}
