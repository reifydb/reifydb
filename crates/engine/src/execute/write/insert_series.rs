// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::Executor;
use reifydb_rql::plan::InsertIntoSeriesPlan;
use reifydb_storage::Storage;
use reifydb_transaction::Tx;

impl<S: Storage> Executor<S> {
    pub(crate) fn insert_into_series(
        &mut self,
        tx: &mut impl Tx<S>,
        plan: InsertIntoSeriesPlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            InsertIntoSeriesPlan::Values { schema, series, columns, rows_to_insert } => {
                // let mut rows = Vec::with_capacity(rows_to_insert.len());
                //
                // for row in rows_to_insert {
                //     let mut row_values = Vec::with_capacity(row.len());
                //     for expr in row {
                //         match expr {
                //             Expression::Constant(value) => row_values.push(value),
                //             _ => unimplemented!(),
                //         }
                //     }
                //     rows.push(row_values);
                // }
                //
                // let result = tx.insert_into_series(schema.as_str(), series.as_str(), rows).unwrap();
                //
                // Ok(ExecutionResult::InsertIntoSeries { schema, series, inserted: result.inserted })
                unimplemented!()
            }
        }
    }
}
