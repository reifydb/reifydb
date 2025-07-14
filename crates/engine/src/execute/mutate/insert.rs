// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Error;
use crate::execute::{Batch, ExecutionContext, Executor, compile};
use crate::frame::Frame;
use reifydb_catalog::{
    Catalog,
    key::{EncodableKey, TableRowKey},
    sequence::TableRowSequence,
};
use reifydb_core::{
    DataType, IntoSpan, Value,
    interface::{Tx, UnversionedStorage, VersionedStorage},
    row::Layout,
};
use reifydb_diagnostic::catalog::table_not_found;
use reifydb_rql::plan::physical::InsertPlan;
use std::sync::Arc;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn insert(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: InsertPlan,
    ) -> crate::Result<Frame> {
        let schema_name = plan.schema.as_ref().map(|s| s.fragment.as_str()).unwrap(); // FIXME

        let schema = Catalog::get_schema_by_name(tx, schema_name)?.unwrap();
        let Some(table) = Catalog::get_table_by_name(tx, schema.id, &plan.table.fragment)? else {
            let span = plan.table.into_span();
            return Err(Error::execution(table_not_found(
                span.clone(),
                schema_name,
                &span.fragment,
            )));
        };

        let table_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type).collect();
        let layout = Layout::new(&table_types);

        // Compile the input plan into an execution node with table context
        let context = ExecutionContext::with_table(self.functions.clone(), table.clone());
        let mut input_node = compile(*plan.input, tx, Arc::new(context));

        let mut inserted_count = 0;

        // Process all input batches using volcano iterator pattern
        while let Some(Batch { frame, mask }) = input_node.next()? {
            let row_count = frame.row_count();

            for row_idx in 0..row_count {
                if !mask.get(row_idx) {
                    continue;
                }

                let mut row = layout.allocate_row();

                // For each table column, find if it exists in the input frame
                for (table_idx, table_column) in table.columns.iter().enumerate() {
                    let value = if let Some(input_column) =
                        frame.columns.iter().find(|col| col.name == table_column.name)
                    {
                        input_column.values.get(row_idx)
                    } else {
                        Value::Undefined
                    };

                    match value {
                        Value::Bool(v) => layout.set_bool(&mut row, table_idx, v),
                        Value::Float4(v) => layout.set_f32(&mut row, table_idx, *v),
                        Value::Float8(v) => layout.set_f64(&mut row, table_idx, *v),
                        Value::Int1(v) => layout.set_i8(&mut row, table_idx, v),
                        Value::Int2(v) => layout.set_i16(&mut row, table_idx, v),
                        Value::Int4(v) => layout.set_i32(&mut row, table_idx, v),
                        Value::Int8(v) => layout.set_i64(&mut row, table_idx, v),
                        Value::Int16(v) => layout.set_i128(&mut row, table_idx, v),
                        Value::Utf8(v) => layout.set_utf8(&mut row, table_idx, v),
                        Value::Uint1(v) => layout.set_u8(&mut row, table_idx, v),
                        Value::Uint2(v) => layout.set_u16(&mut row, table_idx, v),
                        Value::Uint4(v) => layout.set_u32(&mut row, table_idx, v),
                        Value::Uint8(v) => layout.set_u64(&mut row, table_idx, v),
                        Value::Uint16(v) => layout.set_u128(&mut row, table_idx, v),
                        Value::Undefined => layout.set_undefined(&mut row, table_idx),
                    }
                }

                // Insert the row into the database
                let row_id = TableRowSequence::next_row_id(tx, table.id)?;
                tx.set(&TableRowKey { table: table.id, row: row_id }.encode(), row).unwrap();

                inserted_count += 1;
            }
        }

        // Return summary frame
        Ok(Frame::single_row([
            ("schema", Value::Utf8(schema.name)),
            ("table", Value::Utf8(table.name)),
            ("inserted", Value::Uint8(inserted_count as u64)),
        ]))
    }
}
