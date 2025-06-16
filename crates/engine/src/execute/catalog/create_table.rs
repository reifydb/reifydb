// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use crate::{CreateTableResult, Error};
use reifydb_catalog::Catalog;
use reifydb_catalog::table::TableToCreate;
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
        let Some(schema) = Catalog::get_schema_by_name(tx, &plan.schema)? else {
            return Err(Error::execution(Diagnostic::schema_not_found(
                Some(plan.span),
                &plan.schema,
            )));
        };

        if let Some(table) = Catalog::get_table_by_name(tx, schema.id, &plan.table)? {
            if plan.if_not_exists {
                return Ok(ExecutionResult::CreateTable(CreateTableResult {
                    id: table.id,
                    schema: plan.schema,
                    table: plan.table,
                    created: false,
                }));
            }

            return Err(Error::execution(Diagnostic::table_already_exists(
                Some(plan.span),
                &schema.name,
                &table.name,
            )));
        }

        let table = Catalog::create_table(
            tx,
            TableToCreate {
                span: Some(plan.span),
                table: plan.table,
                schema: plan.schema.clone(),
                columns: plan.columns,
            },
        )?;

        Ok(ExecutionResult::CreateTable(CreateTableResult {
            id: table.id,
            schema: plan.schema,
            table: table.name,
            created: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::CreateTableResult;
    use crate::execute::catalog::create_table::CreateTablePlan;
    use crate::{ExecutionResult, execute_tx};
    use reifydb_catalog::table::TableId;
    use reifydb_catalog::test_utils::{create_schema, ensure_test_schema};
    use reifydb_diagnostic::Span;
    use reifydb_rql::plan::PlanTx;
    use reifydb_transaction::test_utils::TestTransaction;

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
    fn test_create_same_table_in_different_schema() {
        let mut tx = TestTransaction::new();

        ensure_test_schema(&mut tx);
        create_schema(&mut tx, "another_schema");

        let 
        plan = CreateTablePlan {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

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

        let plan = CreateTablePlan {
            schema: "another_schema".to_string(),
            table: "test_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

        let result = execute_tx(&mut tx, PlanTx::CreateTable(plan.clone())).unwrap();
        assert_eq!(
            result,
            ExecutionResult::CreateTable(CreateTableResult {
                id: TableId(2),
                schema: "another_schema".into(),
                table: "test_table".into(),
                created: true
            })
        );
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
