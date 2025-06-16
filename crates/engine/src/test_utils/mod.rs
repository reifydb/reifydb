// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[cfg(test)]
use crate::ColumnToCreate;
#[cfg(test)]
use crate::execute::Executor;
#[cfg(test)]
use crate::{CreateSchemaResult, CreateTableResult, ExecutionResult, execute_tx};
#[cfg(test)]
use reifydb_catalog::ColumnPolicy;
#[cfg(test)]
use reifydb_core::ValueKind;
#[cfg(test)]
use reifydb_core::catalog::TableId;
#[cfg(test)]
use reifydb_diagnostic::Span;
#[cfg(test)]
use reifydb_rql::plan::{CreateSchemaPlan, CreateTablePlan, PlanTx};
#[cfg(test)]
use reifydb_storage::memory::Memory;
#[cfg(test)]
use reifydb_transaction::Tx;

#[cfg(test)]
pub fn create_schema(tx: &mut impl Tx<Memory, Memory>, schema: &str) -> CreateSchemaResult {
    let schema_plan = CreateSchemaPlan {
        schema: schema.to_string(),
        if_not_exists: false,
        span: Span::testing(),
    };

    match execute_tx(tx, PlanTx::CreateSchema(schema_plan)).unwrap() {
        ExecutionResult::CreateSchema(result) => result,
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub fn ensure_test_schema(tx: &mut impl Tx<Memory, Memory>) -> CreateSchemaResult {
    let schema_plan = CreateSchemaPlan {
        schema: "test_schema".to_string(),
        if_not_exists: true,
        span: Span::testing(),
    };

    match execute_tx(tx, PlanTx::CreateSchema(schema_plan)).unwrap() {
        ExecutionResult::CreateSchema(result) => result,
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub fn ensure_test_table(tx: &mut impl Tx<Memory, Memory>) -> CreateTableResult {
    ensure_test_schema(tx);
    create_table(tx, "test_schema", "test_table", &[])
}

#[cfg(test)]
pub fn create_table(
    tx: &mut impl Tx<Memory, Memory>,
    schema: &str,
    table: &str,
    columns: &[reifydb_catalog::ColumnToCreate],
) -> CreateTableResult {
    let table_plan = CreateTablePlan {
        schema: schema.to_string(),
        table: table.to_string(),
        if_not_exists: true,
        columns: columns.to_vec(),
        span: Span::testing(),
    };

    match execute_tx(tx, PlanTx::CreateTable(table_plan)).unwrap() {
        ExecutionResult::CreateTable(result) => result,
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub fn create_test_table_column(
    tx: &mut impl Tx<Memory, Memory>,
    name: &str,
    value: ValueKind,
    policies: Vec<ColumnPolicy>,
) {
    ensure_test_table(tx);

    let mut executor = Executor::testing();
    executor
        .create_column(
            tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: name.to_string(),
                value,
                if_not_exists: false,
                policies,
            },
        )
        .unwrap();
}
