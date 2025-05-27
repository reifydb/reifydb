// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use rql::plan::CreateSeriesPlan;
use transaction::{SchemaTx, StoreToCreate, Tx};

impl Executor {
    pub(crate) fn create_series(
        &mut self,
        tx: &mut impl Tx,
        plan: CreateSeriesPlan,
    ) -> crate::Result<ExecutionResult> {
        if plan.if_not_exists {
            unimplemented!()
        } else {
            tx.schema_mut(&plan.schema)?.create(StoreToCreate::Series {
                series: plan.series.clone(),
                columns: plan.columns,
            })?;
        }
        Ok(ExecutionResult::CreateSeries { schema: plan.schema, series: plan.series })
    }
}
