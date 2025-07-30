// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use crate::column::frame::Frame;
use crate::execute::mutate::coerce::coerce_value_to_column_type;
use crate::execute::{Batch, ExecutionContext, Executor, compile};
use reifydb_catalog::Catalog;
use reifydb_core::error::diagnostic::catalog::{schema_not_found, table_not_found};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::interface::{EncodableKey, TableRowKey};
use reifydb_core::{
    ColumnDescriptor, IntoOwnedSpan, Type, Value,
    interface::{ColumnPolicyKind, Tx, UnversionedStorage, VersionedStorage},
    return_error,
    row::Layout,
    value::row_id::ROW_ID_COLUMN_NAME,
};
use reifydb_rql::plan::physical::UpdatePlan;
use std::sync::Arc;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn update(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: UpdatePlan,
    ) -> crate::Result<Frame> {
        let Some(schema_ref) = plan.schema.as_ref() else {
            return_error!(schema_not_found(None::<reifydb_core::OwnedSpan>, "default"));
        };
        let schema_name = schema_ref.fragment.as_str();

        let schema = Catalog::get_schema_by_name(tx, schema_name)?.unwrap();
        let Some(table) = Catalog::get_table_by_name(tx, schema.id, &plan.table.fragment)? else {
            let span = plan.table.into_span();
            return_error!(table_not_found(span.clone(), schema_name, &span.fragment,));
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
        while let Some(Batch { frame }) = input_node.next(&context, tx)? {
            // Find the RowId column - return error if not found
            let Some(row_id_column) =
                frame.columns.iter().find(|col| col.name() == ROW_ID_COLUMN_NAME)
            else {
                return_error!(engine::missing_row_id_column());
            };

            // Extract RowId data - panic if any are undefined
            let row_ids = match &row_id_column.data() {
                EngineColumnData::RowId(container) => {
                    // Check that all row IDs are defined
                    for i in 0..container.data().len() {
                        if !container.is_defined(i) {
                            return_error!(engine::invalid_row_id_values());
                        }
                    }
                    container.data()
                }
                _ => return_error!(engine::invalid_row_id_values()),
            };

            let row_count = frame.row_count();

            for row_idx in 0..row_count {
                let mut row = layout.allocate_row();

                // For each table column, find if it exists in the input frame
                for (table_idx, table_column) in table.columns.iter().enumerate() {
                    let mut value = if let Some(input_column) =
                        frame.columns.iter().find(|col| col.name() == table_column.name)
                    {
                        input_column.data().get_value(row_idx)
                    } else {
                        Value::Undefined
                    };

                    // Apply automatic type coercion
                    // Extract policies (no conversion needed since types are now unified)
                    let policies: Vec<ColumnPolicyKind> =
                        table_column.policies.iter().map(|cp| cp.policy.clone()).collect();

                    value = coerce_value_to_column_type(
                        value,
                        table_column.ty,
                        ColumnDescriptor::new()
                            .with_schema(&schema.name)
                            .with_table(&table.name)
                            .with_column(&table_column.name)
                            .with_column_type(table_column.ty)
                            .with_policies(policies),
                    )?;

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
                        Value::RowId(_v) => {}
                        Value::Uuid4(v) => layout.set_uuid(&mut row, table_idx, *v),
                        Value::Uuid7(v) => layout.set_uuid(&mut row, table_idx, *v),
                        Value::Blob(v) => layout.set_blob(&mut row, table_idx, &v),
                        Value::Undefined => layout.set_undefined(&mut row, table_idx),
                    }
                }

                // Update the row using the existing RowId from the frame
                let row_id = row_ids[row_idx];
                tx.set(&TableRowKey { table: table.id, row: row_id }.encode(), row)?;

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
