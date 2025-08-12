// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::ColumnData;
use crate::columnar::Columns;
use crate::execute::mutate::coerce::coerce_value_to_column_type;
use crate::execute::{Batch, ExecutionContext, Executor, compile};
use reifydb_catalog::Catalog;
use reifydb_core::interface::{Transaction, 
    ActiveCommandTransaction, EncodableKey, Params, TableRowKey,
};
use reifydb_core::result::error::diagnostic::catalog::{schema_not_found, table_not_found};
use reifydb_core::result::error::diagnostic::engine;
use reifydb_core::{
    ColumnDescriptor, IntoOwnedSpan, Type, Value,
    interface::{ColumnPolicyKind, VersionedCommandTransaction},
    return_error,
    row::EncodedRowLayout,
    value::row_id::ROW_ID_COLUMN_NAME,
};
use reifydb_rql::plan::physical::UpdatePlan;
use std::sync::Arc;

impl<T: Transaction> Executor<T> {
    pub(crate) fn update(
        &self,
        txn: &mut ActiveCommandTransaction<T>,
        plan: UpdatePlan,
        params: Params,
    ) -> crate::Result<Columns> {
        let Some(schema_ref) = plan.schema.as_ref() else {
            return_error!(schema_not_found(None::<reifydb_core::OwnedSpan>, "default"));
        };
        let schema_name = schema_ref.fragment.as_str();

        let schema = Catalog::get_schema_by_name(txn, schema_name)?.unwrap();
        let Some(table) = Catalog::get_table_by_name(txn, schema.id, &plan.table.fragment)? else {
            let span = plan.table.into_span();
            return_error!(table_not_found(span.clone(), schema_name, &span.fragment,));
        };

        let table_types: Vec<Type> = table.columns.iter().map(|c| c.ty).collect();
        let layout = EncodedRowLayout::new(&table_types);

        // Compile the input plan into an execution node with table context
        let mut input_node = compile(
            *plan.input,
            txn,
            Arc::new(ExecutionContext {
                functions: self.functions.clone(),
                table: Some(table.clone()),
                batch_size: 1024,
                preserve_row_ids: true,
                params: params.clone(),
            }),
        );

        let mut updated_count = 0;

        // Process all input batches using volcano iterator pattern
        let context = ExecutionContext {
            functions: self.functions.clone(),
            table: Some(table.clone()),
            batch_size: 1024,
            preserve_row_ids: true,
            params: params.clone(),
        };
        while let Some(Batch { columns }) = input_node.next(&context, txn)? {
            // Find the RowId column - return error if not found
            let Some(row_id_column) = columns.iter().find(|col| col.name() == ROW_ID_COLUMN_NAME)
            else {
                return_error!(engine::missing_row_id_column());
            };

            // Extract RowId data - panic if any are undefined
            let row_ids = match &row_id_column.data() {
                ColumnData::RowId(container) => {
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

            let row_count = columns.row_count();

            for row_idx in 0..row_count {
                let mut row = layout.allocate_row();

                // For each table column, find if it exists in the input columns
                for (table_idx, table_column) in table.columns.iter().enumerate() {
                    let mut value = if let Some(input_column) =
                        columns.iter().find(|col| col.name() == table_column.name)
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
                        &context,
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
                        Value::Uuid4(v) => layout.set_uuid4(&mut row, table_idx, v),
                        Value::Uuid7(v) => layout.set_uuid7(&mut row, table_idx, v),
                        Value::Blob(v) => layout.set_blob(&mut row, table_idx, &v),
                        Value::Undefined => layout.set_undefined(&mut row, table_idx),
                    }
                }

                // Update the row using the existing RowId from the columns
                let row_id = row_ids[row_idx];
                txn.set(&TableRowKey { table: table.id, row: row_id }.encode(), row)?;

                updated_count += 1;
            }
        }

        // Return summary columns
        Ok(Columns::single_row([
            ("schema", Value::Utf8(schema.name)),
            ("table", Value::Utf8(table.name)),
            ("updated", Value::Uint8(updated_count as u64)),
        ]))
    }
}
