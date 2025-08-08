// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::Executor;
use catalog::schema_not_found;
use reifydb_catalog::Catalog;
use reifydb_catalog::sequence::ColumnSequence;
use reifydb_core::diagnostic::catalog;
use reifydb_core::diagnostic::catalog::table_not_found;
use reifydb_core::diagnostic::query::column_not_found;
use reifydb_core::diagnostic::sequence::can_not_alter_not_auto_increment;
use reifydb_core::interface::{
    ActiveCommandTransaction, Params, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::{ColumnDescriptor, Value, return_error};
use reifydb_rql::plan::physical::AlterSequencePlan;

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn alter_sequence(
        &self,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        plan: AlterSequencePlan,
    ) -> crate::Result<Columns> {
        let schema_name = match &plan.schema {
            Some(schema) => schema.as_ref(),
            None => unimplemented!(),
        };

        let Some(schema) = Catalog::get_schema_by_name(txn, schema_name)? else {
            return_error!(schema_not_found(plan.schema.clone(), schema_name,));
        };

        let Some(table) = Catalog::get_table_by_name(txn, schema.id, &plan.table)? else {
            return_error!(table_not_found(plan.table.clone(), &schema.name, &plan.table.as_ref(),));
        };

        let Some(column) = Catalog::get_column_by_name(txn, table.id, plan.column.as_ref())? else {
            return_error!(column_not_found(plan.column.clone()));
        };

        if !column.auto_increment {
            return_error!(can_not_alter_not_auto_increment(plan.column));
        }

        // For catalog operations, use empty params since no ExecutionContext is available
        let empty_params = Params::None;
        let value = evaluate(
            &plan.value,
            &EvaluationContext {
                target_column: Some(ColumnDescriptor {
                    schema: None,
                    table: None,
                    column: None,
                    column_type: Some(column.ty.clone()),
                    policies: vec![],
                }),
                column_policies: vec![],
                columns: Columns::empty(),
                row_count: 1,
                take: None,
                params: &empty_params,
            },
        )?;

        let data = value.data();
        debug_assert_eq!(data.len(), 1);

        let value = data.get_value(0);
        ColumnSequence::set_value(txn, table.id, column.id, value.clone())?;

        Ok(Columns::single_row([
            ("schema", Value::Utf8(schema.name)),
            ("table", Value::Utf8(table.name)),
            ("column", Value::Utf8(column.name)),
            ("value", value),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::Executor;
    use ConstantExpression::Number;
    use Expression::Constant;
    use reifydb_catalog::Catalog;
    use reifydb_catalog::table::{ColumnToCreate, TableToCreate};
    use reifydb_catalog::test_utils::ensure_test_schema;
    use reifydb_core::interface::Params;
    use reifydb_core::{OwnedSpan, Type, Value};
    use reifydb_rql::expression::{ConstantExpression, Expression};
    use reifydb_rql::plan::physical::{AlterSequencePlan, PhysicalPlan};
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_ok() {
        let mut txn = create_test_command_transaction();
        ensure_test_schema(&mut txn);

        // Create a table with an auto-increment column
        Catalog::create_table(
            &mut txn,
            TableToCreate {
                span: None,
                schema: "test_schema".to_string(),
                table: "users".to_string(),
                columns: vec![
                    ColumnToCreate {
                        span: None,
                        name: "id".to_string(),
                        ty: Type::Int4,
                        policies: vec![],
                        auto_increment: true,
                    },
                    ColumnToCreate {
                        span: None,
                        name: "name".to_string(),
                        ty: Type::Utf8,
                        policies: vec![],
                        auto_increment: false,
                    },
                ],
            },
        )
        .unwrap();

        // Alter the sequence to start at 1000
        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("test_schema")),
            table: OwnedSpan::testing("users"),
            column: OwnedSpan::testing("id"),
            value: Constant(Number { span: OwnedSpan::testing("1000") }),
        };

        let result = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
            .unwrap();

        assert_eq!(result.row(0)[0], Value::Utf8("test_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("users".to_string()));
        assert_eq!(result.row(0)[2], Value::Utf8("id".to_string()));
        assert_eq!(result.row(0)[3], Value::Int4(1000));
    }

    #[test]
    fn test_non_auto_increment_column() {
        let mut txn = create_test_command_transaction();
        ensure_test_schema(&mut txn);

        // Create a table with a non-auto-increment column
        Catalog::create_table(
            &mut txn,
            TableToCreate {
                span: None,
                schema: "test_schema".to_string(),
                table: "items".to_string(),
                columns: vec![ColumnToCreate {
                    span: None,
                    name: "id".to_string(),
                    ty: Type::Int4,
                    policies: vec![],
                    auto_increment: false,
                }],
            },
        )
        .unwrap();

        // Try to alter sequence on non-auto-increment column
        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("test_schema")),
            table: OwnedSpan::testing("items"),
            column: OwnedSpan::testing("id"),
            value: Constant(Number { span: OwnedSpan::testing("100") }),
        };

        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
            .unwrap_err();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "SEQUENCE_002");
    }

    #[test]
    fn test_schema_not_found() {
        let mut txn = create_test_command_transaction();

        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("non_existent_schema")),
            table: OwnedSpan::testing("some_table"),
            column: OwnedSpan::testing("id"),
            value: Constant(Number { span: OwnedSpan::testing("1000") }),
        };

        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
            .unwrap_err();

        assert_eq!(err.diagnostic().code, "CA_002");
    }

    #[test]
    fn test_table_not_found() {
        let mut txn = create_test_command_transaction();
        ensure_test_schema(&mut txn);

        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("test_schema")),
            table: OwnedSpan::testing("non_existent_table"),
            column: OwnedSpan::testing("id"),
            value: Constant(Number { span: OwnedSpan::testing("1000") }),
        };

        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
            .unwrap_err();

        assert_eq!(err.diagnostic().code, "CA_004");
    }

    #[test]
    fn test_column_not_found() {
        let mut txn = create_test_command_transaction();
        ensure_test_schema(&mut txn);

        // Create a table
        Catalog::create_table(
            &mut txn,
            TableToCreate {
                span: None,
                schema: "test_schema".to_string(),
                table: "posts".to_string(),
                columns: vec![ColumnToCreate {
                    span: None,
                    name: "id".to_string(),
                    ty: Type::Int4,
                    policies: vec![],
                    auto_increment: true,
                }],
            },
        )
        .unwrap();

        // Try to alter sequence on non-existent column
        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("test_schema")),
            table: OwnedSpan::testing("posts"),
            column: OwnedSpan::testing("non_existent_column"),
            value: Constant(Number { span: OwnedSpan::testing("1000") }),
        };

        let err = Executor::testing()
            .execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
            .unwrap_err();

        assert_eq!(err.diagnostic().code, "QUERY_001");
    }
}
