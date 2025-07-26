// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, Executor, compile};
use reifydb_catalog::Catalog;
use reifydb_core::error::diagnostic::catalog::{schema_not_found, table_not_found};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::frame::{ColumnValues, Frame};
use reifydb_core::interface::{EncodableKey, EncodableKeyRange, TableRowKey, TableRowKeyRange};
use reifydb_core::{
    EncodedKeyRange, IntoOwnedSpan, Value,
    interface::{Tx, UnversionedStorage, VersionedStorage},
    return_error,
    value::row_id::ROW_ID_COLUMN_NAME,
};
use reifydb_rql::plan::physical::DeletePlan;
use std::collections::Bound::Included;
use std::sync::Arc;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn delete(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: DeletePlan,
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

        let mut deleted_count = 0;

        if let Some(input_plan) = plan.input {
            // Delete specific rows based on input plan
            let mut input_node = compile(
                *input_plan,
                tx,
                Arc::new(ExecutionContext {
                    functions: self.functions.clone(),
                    table: Some(table.clone()),
                    batch_size: 1024,
                    preserve_row_ids: true,
                }),
            );

            let context = ExecutionContext {
                functions: self.functions.clone(),
                table: Some(table.clone()),
                batch_size: 1024,
                preserve_row_ids: true,
            };

            while let Some(Batch { frame, mask }) = input_node.next(&context, tx)? {
                // Find the RowId column - return error if not found
                let Some(row_id_column) =
                    frame.columns.iter().find(|col| col.name() == ROW_ID_COLUMN_NAME)
                else {
                    return_error!(engine::missing_row_id_column());
                };

                // Extract RowId values - return error if any are undefined
                let row_ids = match &row_id_column.values() {
                    ColumnValues::RowId(row_ids, bitvec) => {
                        // Check that all row IDs are defined
                        for i in 0..row_ids.len() {
                            if !bitvec.get(i) {
                                return_error!(engine::invalid_row_id_values());
                            }
                        }
                        row_ids
                    }
                    _ => return_error!(engine::invalid_row_id_values()),
                };

                for row_idx in 0..frame.row_count() {
                    if !mask.get(row_idx) {
                        continue;
                    }

                    // Delete the row using the existing RowId from the frame
                    let row_id = row_ids[row_idx];
                    tx.remove(&TableRowKey { table: table.id, row: row_id }.encode())?;

                    deleted_count += 1;
                }
            }
        } else {
            // Delete entire table - scan all rows and delete them

            let range = TableRowKeyRange { table: table.id };

            let keys = tx
                .scan_range(EncodedKeyRange::new(
                    Included(range.start().unwrap()),
                    Included(range.end().unwrap()),
                ))?
                .map(|versioned| versioned.key)
                .collect::<Vec<_>>();
            for key in keys {
                tx.remove(&key)?;
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
