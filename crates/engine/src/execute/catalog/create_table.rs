// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use crate::execute::catalog::layout::table;
use crate::execute::{CreateTableResult, ExecutionResult, Executor};
use reifydb_core::{Key, TableKey};
use reifydb_diagnostic::Diagnostic;
use reifydb_rql::plan::CreateTablePlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    // FIXME table name already exists
    // FIXME handle create if_not_exists
    // FIXME serialize table and insert
    // FIXME link table to schema

    // let table_layout = Layout::new(&[ValueKind::String]);
    // let mut row = table_layout.allocate_row();
    // table_layout.set_str(&mut row, 0, &plan.table);
    //
    // let table_id = self.next_table_id(tx)?;
    //
    // tx.set(&Key::Table(TableKey { table_id }).encode(), EncodedRow(AsyncCowVec::new(vec![])))?;
    //
    // // table columns
    //
    // tx.set(
    // 	&Key::SchemaTableLink(SchemaTableLinkKey { schema_id: schema.id, table_id }).encode(),
    // 	EncodedRow(AsyncCowVec::new(vec![])),
    // )?;
    //
    // Ok(ExecutionResult::CreateTable { schema: "TBD".to_string(), table: plan.table, created: true })

    pub(crate) fn create_table(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateTablePlan,
    ) -> crate::Result<ExecutionResult> {
        let Some(schema) = self.get_schema_by_name(tx, &plan.schema)? else {
            return Err(Error::execution(Diagnostic::schema_not_found(plan.span, &plan.schema)));
        };

        if let Some(table) = self.get_table_by_name(tx, &plan.table)? {
            if plan.if_not_exists {
                return Ok(ExecutionResult::CreateTable(CreateTableResult {
                    id: table.id,
                    schema: plan.schema,
                    table: plan.table,
                    created: false,
                }));
            }

            return Err(Error::execution(Diagnostic::table_already_exists(
                plan.span,
                &schema.name,
                &table.name,
            )));
        }

        let table_id = self.next_table_id(tx)?;

        let mut row = table::LAYOUT.allocate_row();
        table::LAYOUT.set_u32(&mut row, table::ID, table_id);
        table::LAYOUT.set_u32(&mut row, table::SCHEMA, schema.id);
        table::LAYOUT.set_str(&mut row, table::NAME, &plan.table);

        tx.set(&Key::Table(TableKey { table_id }).encode(), row)?;

        Ok(ExecutionResult::CreateTable(CreateTableResult {
            id: table_id,
            schema: plan.schema,
            table: plan.table,
            created: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::CreateTableResult;
    use crate::execute::catalog::create_table::CreateTablePlan;
    use crate::{ExecutionResult, execute_tx};
    use reifydb_core::catalog::TableId;
    use reifydb_diagnostic::Span;
    use reifydb_rql::plan::PlanTx;
    use reifydb_testing::engine::ensure_test_schema;
    use reifydb_testing::transaction::TestTransaction;

    #[test]
    fn test_create_table() {
        let mut tx = TestTransaction::new();

        ensure_test_schema(&mut tx);

        let mut plan = CreateTablePlan {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

        // First creation should succeed
        let result = execute_tx(&mut tx, PlanTx::CreateTable(plan.clone())).unwrap();
        assert_eq!(
            result,
            ExecutionResult::CreateTable(CreateTableResult {
                id: TableId(1),
                schema: "test_schema".into(),
                table: "test_table".into(),
                created: true
            })
        );

        // Creating the same table again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = execute_tx(&mut tx, PlanTx::CreateTable(plan.clone())).unwrap();
        assert_eq!(
            result,
            ExecutionResult::CreateTable(CreateTableResult {
                id: TableId(1),
                schema: "test_schema".into(),
                table: "test_table".into(),
                created: false
            })
        );

        // Creating the same table again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = execute_tx(&mut tx, PlanTx::CreateTable(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_003");
    }

    #[test]
    fn test_create_table_missing_schema() {
        let mut tx = TestTransaction::new();

        let plan = CreateTablePlan {
            schema: "missing_schema".to_string(),
            table: "my_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

        let err = execute_tx(&mut tx, PlanTx::CreateTable(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_002");
    }
}
