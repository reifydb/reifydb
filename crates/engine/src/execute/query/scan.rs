// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use reifydb_catalog::{SchemaRx, StoreRx};
use reifydb_storage::Storage;
use reifydb_transaction::Rx;

impl<S: Storage> Executor<S> {
    pub(crate) fn scan(
        &mut self,
        rx: &mut impl Rx<S>,
        schema: &str,
        store: &str,
    ) -> crate::Result<()> {
        unimplemented!()
        // let columns = rx.dep_schema(schema)?.get(store)?.list_columns()?;
        //
        // let values = columns.iter().map(|c| c.value).collect::<Vec<_>>();
        // let layout = Layout::new(&values);
        //
        // let columns: Vec<Column> = columns
        //     .iter()
        //     .map(|col| {
        //         let name = col.name.clone();
        //         let data = match col.value {
        //             ValueKind::Bool => ColumnValues::bool(vec![]),
        //             ValueKind::Float4 => ColumnValues::float4(vec![]),
        //             ValueKind::Float8 => ColumnValues::float8(vec![]),
        //             ValueKind::Int1 => ColumnValues::int1(vec![]),
        //             ValueKind::Int2 => ColumnValues::int2(vec![]),
        //             ValueKind::Int4 => ColumnValues::int4(vec![]),
        //             ValueKind::Int8 => ColumnValues::int8(vec![]),
        //             ValueKind::Int16 => ColumnValues::int16(vec![]),
        //             ValueKind::String => ColumnValues::string(vec![]),
        //             ValueKind::Uint1 => ColumnValues::uint1(vec![]),
        //             ValueKind::Uint2 => ColumnValues::uint2(vec![]),
        //             ValueKind::Uint4 => ColumnValues::uint4(vec![]),
        //             ValueKind::Uint8 => ColumnValues::uint8(vec![]),
        //             ValueKind::Uint16 => ColumnValues::uint16(vec![]),
        //             ValueKind::Undefined => ColumnValues::Undefined(0),
        //         };
        //         Column { name, data }
        //     })
        //     .collect();
        //
        // let mut frame = Frame::new(columns);
        // frame.append_rows(&layout, rx.dep_scan_table(schema, store)?.into_iter())?;
        // self.frame = frame;
        // Ok(())
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
