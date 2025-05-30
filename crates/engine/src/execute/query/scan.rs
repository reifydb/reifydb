// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use reifydb_core::ValueKind;
use reifydb_frame::{Append, Column, ColumnValues, Frame};
use reifydb_transaction::{Rx, SchemaRx, StoreRx};

impl Executor {
    pub(crate) fn scan(&mut self, rx: &mut impl Rx, schema: &str, store: &str) -> crate::Result<()> {
        let columns = rx.schema(schema)?.get(store)?.list_columns()?;

        let columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let data = match col.value {
                    ValueKind::Int2 => ColumnValues::int2(vec![]),
                    ValueKind::Text => ColumnValues::text(vec![]),
                    ValueKind::Bool => ColumnValues::bool(vec![]),
                    _ => ColumnValues::Undefined(0),
                };
                Column { name, data }
            })
            .collect();

        let mut frame = Frame::new(columns);
        for row in rx.scan_table(schema, store)?.into_iter() {
            frame.append(row)?;
        }

        self.frame = frame;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
