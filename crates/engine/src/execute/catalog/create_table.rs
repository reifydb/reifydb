// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::row::{EncodedRow, Layout};
use reifydb_core::{AsyncCowVec, Key, SchemaTableLinkKey, TableKey, ValueKind};
use reifydb_rql::plan::CreateTablePlan;
use reifydb_storage::Storage;
use reifydb_transaction::Tx;

impl<S: Storage> Executor<S> {
    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx<S>,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        // FIXME schema does not exist
        // FIXME get schema - does not exists
        // FIXME table name already exists
        // FIXME handle create if_not_exists
        // FIXME serialize table and insert
        // FIXME link table to schema

        let table_layout = Layout::new(&[ValueKind::String]);
        let mut row = table_layout.allocate_row();
        table_layout.set_str(&mut row, 0, &plan.table);

        tx.set(
            Key::Table(TableKey { table_id: TableId(1) }).encode(),
            EncodedRow(AsyncCowVec::new(vec![])),
        )?;
        
        // table columns

        tx.set(
            Key::SchemaTableLink(SchemaTableLinkKey {
                schema_id: SchemaId(1),
                table_id: TableId(1),
            })
            .encode(),
            EncodedRow(AsyncCowVec::new(vec![])),
        )?;

        Ok(ExecutionResult::CreateTable { schema: "TBD".to_string(), table: plan.table })
    }
}
