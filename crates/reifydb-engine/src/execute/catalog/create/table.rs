// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::execute::Executor;
use reifydb_catalog::Catalog;
use reifydb_catalog::table::TableToCreate;
use reifydb_core::interface::{
    ActiveCommandTransaction, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::error::diagnostic::catalog::{schema_not_found, table_already_exists};
use reifydb_core::{Value, return_error};
use reifydb_rql::plan::physical::CreateTablePlan;

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn create_table(
        &self,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        plan: CreateTablePlan,
    ) -> crate::Result<Columns> {
        let Some(schema) = Catalog::get_schema_by_name(txn, &plan.schema)? else {
            return_error!(schema_not_found(Some(plan.schema.clone()), &plan.schema.as_ref(),));
        };

        if let Some(table) = Catalog::get_table_by_name(txn, schema.id, &plan.table)? {
            if plan.if_not_exists {
                return Ok(Columns::single_row([
                    ("schema", Value::Utf8(plan.schema.to_string())),
                    ("table", Value::Utf8(plan.table.to_string())),
                    ("created", Value::Bool(false)),
                ]));
            }

            return_error!(table_already_exists(
                Some(plan.table.clone()),
                &schema.name,
                &table.name,
            ));
        }

        Catalog::create_table(
            txn,
            TableToCreate {
                span: Some(plan.table.clone()),
                table: plan.table.to_string(),
                schema: plan.schema.to_string(),
                columns: plan.columns,
            },
        )?;

        Ok(Columns::single_row([
            ("schema", Value::Utf8(plan.schema.to_string())),
            ("table", Value::Utf8(plan.table.to_string())),
            ("created", Value::Bool(true)),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::Executor;
    use crate::execute::catalog::create::table::CreateTablePlan;
    use reifydb_catalog::test_utils::{create_schema, ensure_test_schema};
    use reifydb_core::interface::Params;
    use reifydb_core::{OwnedSpan, Value};
    use reifydb_rql::plan::physical::PhysicalPlan;
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_create_table() {
        let mut txn = create_test_command_transaction();

        ensure_test_schema(&mut txn);

        let mut plan = CreateTablePlan {
            schema: OwnedSpan::testing("test_schema"),
            table: OwnedSpan::testing("test_table"),
            if_not_exists: false,
            columns: vec![],
        };

        // First creation should succeed
        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateTable(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("test_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
        assert_eq!(result.row(0)[2], Value::Bool(true));

        // Creating the same table again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateTable(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("test_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
        assert_eq!(result.row(0)[2], Value::Bool(false));

        // Creating the same table again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan), Params::default())
            .unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_003");
    }

    #[test]
    fn test_create_same_table_in_different_schema() {
        let mut txn = create_test_command_transaction();

        ensure_test_schema(&mut txn);
        create_schema(&mut txn, "another_schema");

        let plan = CreateTablePlan {
            schema: OwnedSpan::testing("test_schema"),
            table: OwnedSpan::testing("test_table"),
            if_not_exists: false,
            columns: vec![],
        };

        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateTable(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("test_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
        assert_eq!(result.row(0)[2], Value::Bool(true));

        let plan = CreateTablePlan {
            schema: OwnedSpan::testing("another_schema"),
            table: OwnedSpan::testing("test_table"),
            if_not_exists: false,
            columns: vec![],
        };

        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateTable(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("another_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("test_table".to_string()));
        assert_eq!(result.row(0)[2], Value::Bool(true));
    }

    #[test]
    fn test_create_table_missing_schema() {
        let mut txn = create_test_command_transaction();

        let plan = CreateTablePlan {
            schema: OwnedSpan::testing("missing_schema"),
            table: OwnedSpan::testing("my_table"),
            if_not_exists: false,
            columns: vec![],
        };

        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::CreateTable(plan), Params::default())
            .unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_002");
    }
}
