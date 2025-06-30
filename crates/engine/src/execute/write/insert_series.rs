// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ExecutionResult;
use crate::execute::Executor;
use reifydb_rql::plan::InsertIntoSeriesPlan;
use reifydb_core::interface::{Bypass, Tx, UnversionedStorage, VersionedStorage};

impl<VS: VersionedStorage, US: UnversionedStorage, BP: Bypass<US>> Executor<VS, US,BP> {
    pub(crate) fn insert_into_series(
        &mut self,
        _tx: &mut impl Tx<VS, US, BP>,
        plan: InsertIntoSeriesPlan,
    ) -> crate::Result<ExecutionResult> {
        match plan {
            InsertIntoSeriesPlan::Values { .. } => {
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
