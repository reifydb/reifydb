// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_diagnostic::Span;
use reifydb_engine::{CreateSchemaResult, ExecutionResult, execute_tx};
use reifydb_rql::plan::{CreateSchemaPlan, PlanTx};
use reifydb_storage::memory::Memory;
use reifydb_transaction::Tx;

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
