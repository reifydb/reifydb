// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, DataFrame};

impl DataFrame {
    pub fn limit(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let data = match &col.data {
                ColumnValues::Int2(values, valid) => ColumnValues::Int2(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Text(values, valid) => ColumnValues::Text(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Bool(values, valid) => ColumnValues::Bool(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Undefined(len) => ColumnValues::Undefined(n.min(*len)),
            };

            columns.push(Column { name: col.name.clone(), data });
        }

        self.columns = columns;

        Ok(())
    }
}
