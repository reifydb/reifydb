// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #[derive(Debug)]
// pub struct Column {
//     pub name: ColumnName,
//     pub value: ValueType,
//     pub default: Option<Expression>,
// }

use crate::DepColumn;
use reifydb_core::StoreKind;

#[derive(Debug)]
pub struct DepStore {
    pub name: String,
    pub kind: StoreKind,
    pub columns: Vec<DepColumn>,
}

impl crate::DepStoreRx for DepStore {
    fn kind(&self) -> crate::Result<StoreKind> {
        Ok(self.kind)
    }

    fn get_column(&self, column: &str) -> crate::Result<DepColumn> {
        let column_name = column;
        for (idx, column) in self.columns.iter().enumerate() {
            if &column.name == column_name {
                return Ok(column.clone());
            }
        }
        // None
        todo!()
    }

    fn list_columns(&self) -> crate::Result<Vec<DepColumn>> {
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

impl crate::DepStoreTx for DepStore {}
