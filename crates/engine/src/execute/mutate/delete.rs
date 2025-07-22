// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Error;
use crate::execute::{Batch, ExecutionContext, Executor, compile};
use crate::frame::{ColumnValues, Frame};
use reifydb_catalog::{
    Catalog,
    key::{EncodableKey, TableRowKey},
};
use reifydb_core::diagnostic::catalog::table_not_found;
use reifydb_core::{
    IntoOwnedSpan, Value,
    interface::{Tx, UnversionedStorage, VersionedStorage},
    value::row_id::ROW_ID_COLUMN_NAME,
};
use reifydb_rql::plan::physical::DeletePlan;
use std::sync::Arc;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn delete(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: DeletePlan,
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

        let mut deleted_count = 0;

        let context = ExecutionContext {
            functions: self.functions.clone(),
            table: Some(table.clone()),
            batch_size: 1024,
            preserve_row_ids: true,
        };

        while let Some(Batch { frame, mask }) = input_node.next(&context, tx)? {
            let row_id_column = frame
                .columns
                .iter()
                .find(|col| col.name == ROW_ID_COLUMN_NAME)
                .expect("Frame must have a __ROW__ID__ column for DELETE operations");

            let row_ids = match &row_id_column.values {
                ColumnValues::RowId(row_ids, bitvec) => {
                    for i in 0..row_ids.len() {
                        if !bitvec.get(i) {
                            panic!("All RowId values must be defined for DELETE operations");
                        }
                    }
                    row_ids
                }
                _ => panic!("RowId column must contain RowId values"),
            };

            for row_idx in 0..frame.row_count() {
                if !mask.get(row_idx) {
                    continue;
                }

                // Delete the row using the existing RowId from the frame
                let row_id = row_ids[row_idx];
                tx.remove(&TableRowKey { table: table.id, row: row_id }.encode()).unwrap();

                deleted_count += 1;
            }
        }

        // Return summary frame
        Ok(Frame::single_row([
            ("schema", Value::Utf8(schema.name)),
            ("table", Value::Utf8(table.name)),
            ("deleted", Value::Uint8(deleted_count as u64)),
        ]))
    }
}
