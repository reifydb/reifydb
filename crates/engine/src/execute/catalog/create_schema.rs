// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Error;
use crate::execute::Executor;
use crate::frame::Frame;
use reifydb_catalog::Catalog;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_core::Value;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_core::diagnostic::catalog::schema_already_exists;
use reifydb_rql::plan::physical::CreateSchemaPlan;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn create_schema(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        plan: CreateSchemaPlan,
    ) -> crate::Result<Frame> {
        if let Some(schema) = Catalog::get_schema_by_name(tx, &plan.schema)? {
            if plan.if_not_exists {
                return Ok(Frame::single_row([
                    ("schema", Value::Utf8(plan.schema.to_string())),
                    ("created", Value::Bool(false)),
                ]));
            }

            return Err(Error::execution(schema_already_exists(
                Some(plan.schema),
                &schema.name,
            )));
        }

        Catalog::create_schema(
            tx,
            SchemaToCreate {
                schema_span: Some(plan.schema.clone()),
                name: plan.schema.to_string(),
            },
        )?;

        Ok(Frame::single_row([
            ("schema", Value::Utf8(plan.schema.to_string())),
            ("created", Value::Bool(true)),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::execute_tx;
    use reifydb_core::{OwnedSpan, Value};
    use reifydb_rql::plan::physical::{CreateSchemaPlan, PhysicalPlan};
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_create_schema() {
        let mut tx = TestTransaction::new();

        let mut plan =
            CreateSchemaPlan { schema: OwnedSpan::testing("my_schema"), if_not_exists: false };

        // First creation should succeed
        let result = execute_tx(&mut tx, PhysicalPlan::CreateSchema(plan.clone())).unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(true));

        // Creating the same schema again with `if_not_exists = true` should not error
        plan.if_not_exists = true;
        let result = execute_tx(&mut tx, PhysicalPlan::CreateSchema(plan.clone())).unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("my_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Bool(false));

        // Creating the same schema again with `if_not_exists = false` should return error
        plan.if_not_exists = false;
        let err = execute_tx(&mut tx, PhysicalPlan::CreateSchema(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_001");
    }
}
