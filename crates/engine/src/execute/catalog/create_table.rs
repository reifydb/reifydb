// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_rql::plan::CreateTablePlan;
use reifydb_transaction::{SchemaTx, StoreToCreate, Tx};

impl Executor {
    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        if plan.if_not_exists {
            unimplemented!()
        } else {
            tx.schema_mut(&plan.schema)?.create(StoreToCreate::Table {
                table: plan.table.clone(),
                columns: plan.columns,
            })?;
        }

        Ok(ExecutionResult::CreateTable { schema: plan.schema, table: plan.table.clone() })
    }
}
