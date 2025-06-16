// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::catalog::layout::schema;
use crate::execute::{CreateSchemaResult, ExecutionResult, Executor};
use crate::Error;
use reifydb_core::{Key, SchemaKey};
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
        if let Some(schema) = self.get_schema_by_name(tx, &plan.schema)? {
            if plan.if_not_exists {
                return Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
                    id: schema.id,
                    schema: plan.schema,
                    created: false,
                }));
            }

            return Err(Error::execution(Diagnostic::schema_already_exists(
                plan.span,
                &schema.name,
            )));
        }

        let schema_id = self.next_schema_id(tx)?;

        let mut row = schema::LAYOUT.allocate_row();
        schema::LAYOUT.set_u32(&mut row, schema::ID, schema_id);
        schema::LAYOUT.set_str(&mut row, schema::NAME, &plan.schema);

        tx.set(&Key::Schema(SchemaKey { schema: schema_id }).encode(), row)?;

        Ok(ExecutionResult::CreateSchema(CreateSchemaResult {
            id: schema_id,
            schema: plan.schema,
            created: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::SchemaId;
    use crate::execute::{execute_tx, CreateSchemaResult};
    use crate::ExecutionResult;
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
        dbg!(err.diagnostic().code, "CA_001");
    }
}
