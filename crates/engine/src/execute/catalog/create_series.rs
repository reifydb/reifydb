// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_rql::plan::CreateSeriesPlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_series(
        &mut self,
        _tx: &mut impl Tx<VS, US>,
        _plan: CreateSeriesPlan,
    ) -> crate::Result<ExecutionResult> {
        // if plan.if_not_exists {
        //     unimplemented!()
        // } else {
        //     tx.dep_schema_mut(&plan.schema)?.create(StoreToCreate::Series {
        //         series: plan.series.clone(),
        //         columns: plan.columns,
        //     })?;
        // }
        // Ok(ExecutionResult::CreateSeries { schema: plan.schema, series: plan.series })

        unimplemented!();
    }
}
