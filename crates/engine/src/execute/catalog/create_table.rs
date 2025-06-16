// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use crate::execute::catalog::layout::{table, table_schema_link};
use crate::execute::{CreateTableResult, ExecutionResult, Executor};
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::{Key, SchemaTableLinkKey, TableKey};
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
        Self::store_table(tx, table_id, schema.id, &plan)?;
        Self::link_table_to_schema(tx, table_id, schema.id)?;

        Ok(ExecutionResult::CreateTable(CreateTableResult {
            id: table_id,
            schema: plan.schema,
            table: plan.table,
            created: true,
        }))
    }

    fn store_table(
        tx: &mut impl Tx<VS, US>,
        id: TableId,
        schema: SchemaId,
        plan: &CreateTablePlan,
    ) -> crate::Result<()> {
        let mut row = table::LAYOUT.allocate_row();
        table::LAYOUT.set_u32(&mut row, table::ID, id);
        table::LAYOUT.set_u32(&mut row, table::SCHEMA, schema);
        table::LAYOUT.set_str(&mut row, table::NAME, &plan.table);

        tx.set(&Key::Table(TableKey { table_id: id }).encode(), row)?;

        Ok(())
    }

    fn link_table_to_schema(
        tx: &mut impl Tx<VS, US>,
        id: TableId,
        schema: SchemaId,
    ) -> crate::Result<()> {
        let mut row = table_schema_link::LAYOUT.allocate_row();
        table_schema_link::LAYOUT.set_u32(&mut row, table_schema_link::ID, id);

        tx.set(
            &Key::SchemaTableLink(SchemaTableLinkKey { schema_id: schema, table_id: id }).encode(),
            row,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::CreateTableResult;
    use crate::execute::catalog::create_table::CreateTablePlan;
    use crate::test_utils::ensure_test_schema;
    use crate::{ExecutionResult, execute_tx};
    use reifydb_core::catalog::{SchemaId, TableId};
    use reifydb_core::row::EncodedRow;
    use reifydb_core::{AsyncCowVec, EncodableKey, SchemaTableLinkKey};
    use reifydb_diagnostic::Span;
    use reifydb_rql::plan::PlanTx;
    use reifydb_transaction::Rx;
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
    fn test_table_linked_to_schema() {
        let mut tx = TestTransaction::new();
        ensure_test_schema(&mut tx);

        let plan = CreateTablePlan {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

        execute_tx(&mut tx, PlanTx::CreateTable(plan)).unwrap();

        let plan = CreateTablePlan {
            schema: "test_schema".to_string(),
            table: "another_table".to_string(),
            if_not_exists: false,
            columns: vec![],
            span: Span::testing(),
        };

        execute_tx(&mut tx, PlanTx::CreateTable(plan)).unwrap();

        let links =
            tx.scan_range(SchemaTableLinkKey::full_scan(SchemaId(1))).unwrap().collect::<Vec<_>>();
        assert_eq!(links.len(), 2);

        let link = &links[0];
        assert_eq!(
            link.key,
            SchemaTableLinkKey { schema_id: SchemaId(1), table_id: TableId(1) }.encode()
        );

        assert_eq!(
            link.row,
            EncodedRow(AsyncCowVec::new([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00].to_vec())) // validity + padding + id
        );

        let link = &links[1];
        assert_eq!(
            link.key,
            SchemaTableLinkKey { schema_id: SchemaId(1), table_id: TableId(2) }.encode()
        );

        assert_eq!(
            link.row,
            EncodedRow(AsyncCowVec::new([0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00].to_vec())) // validity + padding + id
        )
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
