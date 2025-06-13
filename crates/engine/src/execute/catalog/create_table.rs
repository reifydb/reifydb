// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_core::AsyncCowVec;
use reifydb_core::catalog::TableId;
use reifydb_core::row::EncodedRow;
use reifydb_rql::plan::CreateTablePlan;
use reifydb_transaction::Tx;

impl Executor {
    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        // FIXME schema does not exist
        // FIXME get schema - does not exists
        // FIXME table name already exists
        // FIXME handle create if_not_exists
        // FIXME serialize table and insert
        // FIXME link table to schema 
        
        tx.insert_table(TableId(1), EncodedRow(AsyncCowVec::new(vec![])))?;

        Ok(ExecutionResult::CreateTable { schema: "TBD".to_string(), table: plan.table })
    }
}
