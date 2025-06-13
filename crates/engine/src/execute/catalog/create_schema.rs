// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_core::catalog::SchemaId;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, Key, SchemaKey};
use reifydb_rql::plan::CreateSchemaPlan;
use reifydb_storage::Storage;
use reifydb_transaction::Tx;

impl<S: Storage> Executor<S> {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx<S>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<ExecutionResult> {
        // FIXME schema name already exists
        // FIXME handle create if_not_exists
        // FIXME serialize schema and insert
        tx.set(
            Key::Schema(SchemaKey { schema_id: SchemaId(1) }).encode(),
            EncodedRow(AsyncCowVec::new(vec![])),
        )?;

        Ok(ExecutionResult::CreateSchema { schema: plan.schema })
    }
}
