// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::execute::Executor;
use reifydb_catalog::Catalog;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_core::interface::{
    ActiveCommandTransaction, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::result::error::diagnostic::catalog::schema_already_exists;
use reifydb_core::{Value, return_error};
use reifydb_rql::plan::physical::CreateSchemaPlan;

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn create_schema(
        &mut self,
        atx: &mut ActiveCommandTransaction<VT, UT>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<Columns> {
        if let Some(schema) = Catalog::get_schema_by_name(atx, &plan.schema)? {
            if plan.if_not_exists {
                return Ok(Columns::single_row([
                    ("schema", Value::Utf8(plan.schema.to_string())),
                    ("created", Value::Bool(false)),
                ]));
            }

            return_error!(schema_already_exists(Some(plan.schema), &schema.name,));
        }

        Catalog::create_schema(
            atx,
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
    use crate::execute::execute_write;
    use reifydb_core::{OwnedSpan, Value};
    use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_create_schema() {
        let mut atx = create_test_write_transaction();

        let mut plan =
            CreateSchemaPlan { schema: OwnedSpan::testing("my_schema"), if_not_exists: false };

        // First creation should succeed
        let result = execute_write(&mut atx, PhysicalPlan::CreateSchema(plan.clone())).unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(true));

        // Creating the same schema again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = execute_write(&mut atx, PhysicalPlan::CreateSchema(plan.clone())).unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(false));

        // Creating the same schema again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = execute_write(&mut atx, PhysicalPlan::CreateSchema(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_001");
    }
}
