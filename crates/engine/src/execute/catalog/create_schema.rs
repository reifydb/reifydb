// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_catalog::CatalogTx;
use reifydb_rql::plan::CreateSchemaPlan;
use reifydb_transaction::Tx;

impl Executor {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx,
        plan: CreateSchemaPlan,
    ) -> crate::Result<ExecutionResult> {
        if plan.if_not_exists {
            tx.catalog_mut()?.create_if_not_exists(&plan.schema)?;
        } else {
            tx.catalog_mut()?.create(&plan.schema)?;
        }
        Ok(ExecutionResult::CreateSchema { schema: plan.schema })
    }
}
