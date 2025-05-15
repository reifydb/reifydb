// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Column;
use base::schema::StoreName;

// #[derive(Debug)]
// pub struct Column {
//     pub name: ColumnName,
//     pub value: ValueType,
//     pub default: Option<Expression>,
// }

pub struct Store {
    pub name: StoreName,
    pub columns: Vec<Column>,
}

impl base::Store for Store {
    fn get_column(&self, column: impl AsRef<str>) -> base::Result<Column> {
        let column_name = column.as_ref();
        for (idx, column) in self.columns.iter().enumerate() {
            if &column.name == column_name {
                return Ok(column.clone());
            }
        }
        // None
        todo!()
    }

    fn list_columns(&self) -> base::Result<Vec<Column>> {
        Ok(self.columns.clone())
    }

    fn get_column_index(&self, column: impl AsRef<str>) -> base::Result<usize> {
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

impl base::StoreMut for Store {}
