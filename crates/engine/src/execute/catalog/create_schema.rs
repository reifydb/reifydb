// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{ExecutionResult, Executor};
use crate::{CreateSchemaResult, Error};
use reifydb_catalog::Catalog;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_diagnostic::Diagnostic;
use reifydb_rql::plan::CreateSchemaPlan;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<ExecutionResult> {
        if let Some(schema) = Catalog::get_schema_by_name(tx, &plan.schema)? {
            if plan.if_not_exists {
                return Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
                    id: schema.id,
                    schema: plan.schema,
                    created: false,
                }));
            }

            return Err(Error::execution(Diagnostic::schema_already_exists(
                Some(plan.span),
                &schema.name,
            )));
        }

        let result = Catalog::create_schema(
            tx,
            SchemaToCreate { schema_span: Some(plan.span), name: plan.schema },
        )?;

        Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
            id: result.id,
            schema: result.name,
            created: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::ExecutionResult;
    use crate::execute::SchemaId;
    use crate::execute::{CreateSchemaResult, execute_tx};
    use reifydb_diagnostic::Span;
    use reifydb_rql::plan::{CreateSchemaPlan, PlanTx};
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_create_schema() {
        let mut tx = TestTransaction::new();

        let mut plan = CreateSchemaPlan {
            schema: "my_schema".to_string(),
            if_not_exists: false,
            span: Span::testing(),
        };

        // First creation should succeed
        let result = execute_tx(&mut tx, PlanTx::CreateSchema(plan.clone())).unwrap();
        assert_eq!(
            result,
            ExecutionResult::CreateSchema(CreateSchemaResult {
                id: SchemaId(1),
                schema: "my_schema".into(),
                created: true
            })
        );

        // Creating the same schema again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = execute_tx(&mut tx, PlanTx::CreateSchema(plan.clone())).unwrap();
        assert_eq!(
            result,
            ExecutionResult::CreateSchema(CreateSchemaResult {
                id: SchemaId(1),
                schema: "my_schema".into(),
                created: false
            })
        );

        // Creating the same schema again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = execute_tx(&mut tx, PlanTx::CreateSchema(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_001");
    }
}
