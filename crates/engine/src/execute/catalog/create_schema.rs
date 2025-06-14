// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_core::catalog::SchemaId;
use reifydb_core::row::Layout;
use reifydb_core::{Key, SchemaKey, ValueKind};
use reifydb_rql::plan::CreateSchemaPlan;
use reifydb_storage::VersionedStorage;
use reifydb_transaction::Tx;

impl<VS: VersionedStorage> Executor<VS> {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx<VS>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<ExecutionResult> {
        // FIXME schema name already exists
        // FIXME handle create if_not_exists
        // FIXME serialize schema and insert
        let schema_layout = Layout::new(&[ValueKind::String]);
        let mut row = schema_layout.allocate_row();
        schema_layout.set_str(&mut row, 0, &plan.schema);

        let id = self.next_schema_id(tx)?;

        tx.set(Key::Schema(SchemaKey { schema_id: SchemaId(1) }).encode(), row)?;

        Ok(ExecutionResult::CreateSchema { schema: plan.schema })
    }
}
