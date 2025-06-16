// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use crate::execute::{ExecutionResult, Executor};
use reifydb_core::row::{EncodedRow, Layout};
use reifydb_core::{AsyncCowVec, Key, SchemaTableLinkKey, TableKey, ValueKind};
use reifydb_diagnostic::Diagnostic;
use reifydb_rql::plan::CreateTablePlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        let Some(schema) = self.get_schema_by_name(tx, &plan.schema)? else {
            return Err(Error::execution(Diagnostic::schema_not_found(plan.span, &plan.schema)));
        };

        // FIXME table name already exists
        // FIXME handle create if_not_exists
        // FIXME serialize table and insert
        // FIXME link table to schema

        let table_layout = Layout::new(&[ValueKind::String]);
        let mut row = table_layout.allocate_row();
        table_layout.set_str(&mut row, 0, &plan.table);

        let table_id = self.next_table_id(tx)?;

        tx.set(&Key::Table(TableKey { table_id }).encode(), EncodedRow(AsyncCowVec::new(vec![])))?;

        // table columns

        tx.set(
            &Key::SchemaTableLink(SchemaTableLinkKey { schema_id: schema.id, table_id }).encode(),
            EncodedRow(AsyncCowVec::new(vec![])),
        )?;

        Ok(ExecutionResult::CreateTable { schema: "TBD".to_string(), table: plan.table })
    }
}
