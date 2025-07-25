// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::mutate::coerce::coerce_value_to_column_type;
use crate::execute::{Batch, ExecutionContext, Executor, compile};
use reifydb_catalog::{Catalog, sequence::TableRowSequence};
use reifydb_core::error::diagnostic::catalog::table_not_found;
use reifydb_core::frame::Frame;
use reifydb_core::interface::{EncodableKey, TableRowKey};
use reifydb_core::{
    ColumnDescriptor, IntoOwnedSpan, Type, Value,
    interface::{ColumnPolicyKind, Tx, UnversionedStorage, VersionedStorage},
    return_error,
    row::Layout,
};
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
            return_error!(table_not_found(span.clone(), schema_name, &span.fragment,));
        };

        let table_types: Vec<Type> = table.columns.iter().map(|c| c.ty).collect();
        let layout = Layout::new(&table_types);

        let mut input_node = compile(
            *plan.input,
            tx,
            Arc::new(ExecutionContext {
                functions: self.functions.clone(),
                table: Some(table.clone()),
                batch_size: 1024,
                preserve_row_ids: false,
            }),
        );

        let mut inserted_count = 0;

        // Process all input batches using volcano iterator pattern
        while let Some(Batch { frame, mask }) = input_node.next(
            &Arc::new(ExecutionContext {
                functions: self.functions.clone(),
                table: Some(table.clone()),
                batch_size: 1024,
                preserve_row_ids: false,
            }),
            tx,
        )? {
            let row_count = frame.row_count();

            for row_idx in 0..row_count {
                if !mask.get(row_idx) {
                    continue;
                }

                let mut row = layout.allocate_row();

                // For each table column, find if it exists in the input frame
                for (table_idx, table_column) in table.columns.iter().enumerate() {
                    let mut value = if let Some(input_column) =
                        frame.columns.iter().find(|col| col.name() == table_column.name)
                    {
                        input_column.values().get(row_idx)
                    } else {
                        Value::Undefined
                    };

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
                        Value::RowId(v) => layout.set_u64(&mut row, table_idx, v.value()),
                        Value::Uuid4(v) => layout.set_uuid(&mut row, table_idx, *v),
                        Value::Uuid7(v) => layout.set_uuid(&mut row, table_idx, *v),
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
