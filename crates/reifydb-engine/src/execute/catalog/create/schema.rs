// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::execute::Executor;
use reifydb_catalog::Catalog;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_core::interface::{ActiveCommandTransaction, Transaction};
use reifydb_core::result::error::diagnostic::catalog::schema_already_exists;
use reifydb_core::{Value, return_error};
use reifydb_rql::plan::physical::CreateSchemaPlan;

impl<T: Transaction> Executor<T> {
    pub(crate) fn create_schema(
        &self,
        txn: &mut ActiveCommandTransaction<T>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<Columns> {
        if let Some(schema) = Catalog::get_schema_by_name(txn, &plan.schema)? {
            if plan.if_not_exists {
                return Ok(Columns::single_row([
                    ("schema", Value::Utf8(plan.schema.to_string())),
                    ("created", Value::Bool(false)),
                ]));
            }

            return_error!(schema_already_exists(Some(plan.schema), &schema.name,));
        }

        Catalog::create_schema(
            txn,
            SchemaToCreate {
                schema_span: Some(plan.schema.clone()),
                name: plan.schema.to_string(),
            },
        )?;

        Ok(Columns::single_row([
            ("schema", Value::Utf8(plan.schema.to_string())),
            ("created", Value::Bool(true)),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::Executor;
    use reifydb_core::interface::Params;
    use reifydb_core::{OwnedSpan, Value};
    use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_create_schema() {
        let mut txn = create_test_command_transaction();

        let mut plan =
            CreateSchemaPlan { schema: OwnedSpan::testing("my_schema"), if_not_exists: false };

        // First creation should succeed
        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateSchema(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(true));

        // Creating the same schema again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = Executor::testing()
            .execute_command_plan(
                &mut txn,
                PhysicalPlan::CreateSchema(plan.clone()),
                Params::default(),
            )
            .unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(false));

        // Creating the same schema again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::CreateSchema(plan), Params::default())
            .unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_001");
    }
}
