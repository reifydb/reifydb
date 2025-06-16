// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::catalog::layout::schema;
use reifydb_core::SchemaKey;
use reifydb_core::catalog::{Schema, SchemaId};
use reifydb_core::row::EncodedRow;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn get_schema_by_name(
        &mut self,
        tx: &mut impl Tx<VS, US>,
        name: &str,
    ) -> crate::Result<Option<Schema>> {
        Ok(tx.scan_range(SchemaKey::full_scan())?.find_map(|tv| {
            let row: &EncodedRow = &tv.row();
            let schema_name = schema::LAYOUT.get_str(row, schema::NAME);
            if name == schema_name {
                let id = SchemaId(schema::LAYOUT.get_u32(row, schema::ID));
                Some(Schema { id, name: schema_name.to_string() })
            } else {
                None
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::{Executor, execute_tx};
    use reifydb_diagnostic::Span;
    use reifydb_rql::plan::{CreateSchemaPlan, PlanTx};
    use reifydb_storage::memory::Memory;
    use reifydb_testing::transaction::TestTransaction;
    use reifydb_transaction::Tx;

    #[test]
    fn test_by_name_ok() {
        let mut tx = TestTransaction::new();
        create_schema(&mut tx, "test_schema");

        let schema =
            Executor::testing().get_schema_by_name(&mut tx, "test_schema").unwrap().unwrap();

        assert_eq!(schema.id, 1);
        assert_eq!(schema.name, "test_schema");
    }

    #[test]
    fn test_by_name_empty() {
        let mut tx = TestTransaction::new();

        let result = Executor::testing().get_schema_by_name(&mut tx, "test_schema").unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn test_by_name_not_found() {
        let mut tx = TestTransaction::new();
        create_schema(&mut tx, "another_schema");

        let result = Executor::testing().get_schema_by_name(&mut tx, "test_schema").unwrap();

        assert_eq!(result, None);
    }

    fn create_schema(tx: &mut impl Tx<Memory, Memory>, name: &str) {
        let plan = CreateSchemaPlan {
            schema: name.to_string(),
            if_not_exists: false,
            span: Span::testing(),
        };
        execute_tx(tx, PlanTx::CreateSchema(plan.clone())).unwrap();
    }
}
