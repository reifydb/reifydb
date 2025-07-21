// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, Executor, compile};
use crate::frame::{Frame, ColumnValues};
use reifydb_catalog::{
    Catalog,
    key::{EncodableKey, TableRowKey},
};
use reifydb_core::{
    Type, IntoOwnedSpan, Value,
    interface::{Tx, UnversionedStorage, VersionedStorage},
    row::Layout,
    value::row_id::ROW_ID_COLUMN_NAME,
};
use reifydb_core::error::diagnostic::catalog::table_not_found;
use reifydb_rql::plan::physical::UpdatePlan;
use std::sync::Arc;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn update(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: UpdatePlan,
    ) -> crate::Result<Frame> {
        let schema_name = plan.schema.as_ref().map(|s| s.fragment.as_str()).unwrap(); // FIXME

        let schema = Catalog::get_schema_by_name(tx, schema_name)?.unwrap();
        let Some(table) = Catalog::get_table_by_name(tx, schema.id, &plan.table.fragment)? else {
            let span = plan.table.into_span();
            return Err(reifydb_core::Error(table_not_found(
                span.clone(),
                schema_name,
                &span.fragment,
            )));
        };

        let table_types: Vec<Type> = table.columns.iter().map(|c| c.ty).collect();
        let layout = Layout::new(&table_types);

        // Compile the input plan into an execution node with table context
        let mut input_node = compile(
            *plan.input,
            tx,
            Arc::new(ExecutionContext {
                functions: self.functions.clone(),
                table: Some(table.clone()),
                batch_size: 1024,
                preserve_row_ids: true,
            }),
        );

        let mut updated_count = 0;

        // Process all input batches using volcano iterator pattern
        let context = ExecutionContext {
            functions: self.functions.clone(),
            table: Some(table.clone()),
            batch_size: 1024,
            preserve_row_ids: true,
        };
        while let Some(Batch { frame, mask }) = input_node.next(&context, tx)? {
            // Find the RowId column - panic if not found
            let row_id_column = frame.columns.iter()
                .find(|col| col.name == ROW_ID_COLUMN_NAME)
                .expect("Frame must have a __ROW__ID__ column for UPDATE operations");

            // Extract RowId values - panic if any are undefined
            let row_ids = match &row_id_column.values {
                ColumnValues::RowId(row_ids, bitvec) => {
                    // Check that all row IDs are defined
                    for i in 0..row_ids.len() {
                        if !bitvec.get(i) {
                            panic!("All RowId values must be defined for UPDATE operations");
                        }
                    }
                    row_ids
                }
                _ => panic!("RowId column must contain RowId values"),
            };

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

                    dbg!(&value);

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
                        Value::Date(v) => layout.set_date(&mut row, table_idx, v),
                        Value::DateTime(v) => layout.set_datetime(&mut row, table_idx, v),
                        Value::Time(v) => layout.set_time(&mut row, table_idx, v),
                        Value::Interval(v) => layout.set_interval(&mut row, table_idx, v),
                        Value::RowId(v) => layout.set_u64(&mut row, table_idx, v.value()),
                        Value::Undefined => layout.set_undefined(&mut row, table_idx),
                    }
                }

                // Update the row using the existing RowId from the frame
                let row_id = row_ids[row_idx];
                tx.set(&TableRowKey { table: table.id, row: row_id }.encode(), row).unwrap();

                updated_count += 1;
            }
        }

        // Return summary frame
        Ok(Frame::single_row([
            ("schema", Value::Utf8(schema.name)),
            ("table", Value::Utf8(table.name)),
            ("updated", Value::Uint8(updated_count as u64)),
        ]))
    }
}