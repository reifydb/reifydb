// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::execute::Executor;
use catalog::schema_not_found;
use reifydb_catalog::Catalog;
use reifydb_core::diagnostic::catalog;
use reifydb_core::diagnostic::catalog::table_not_found;
use reifydb_core::diagnostic::query::column_not_found;
use reifydb_core::interface::{
    ActiveWriteTransaction, EncodableKey, TableColumnSequenceKey, UnversionedTransaction,
    UnversionedWriteTransaction, VersionedTransaction,
};
use reifydb_core::{Type, Value, return_error};
use reifydb_rql::plan::physical::AlterSequencePlan;

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Executor<VT, UT> {
    pub(crate) fn alter_sequence(
        &mut self,
        atx: &mut ActiveWriteTransaction<VT, UT>,
        plan: AlterSequencePlan,
    ) -> crate::Result<Columns> {
        let schema_name = match &plan.schema {
            Some(schema) => schema.as_ref(),
            None => unimplemented!(),
        };

        let Some(schema) = Catalog::get_schema_by_name(atx, schema_name)? else {
            return_error!(schema_not_found(plan.schema.clone(), schema_name,));
        };

        // Get the table
        let Some(table) = Catalog::get_table_by_name(atx, schema.id, &plan.table)? else {
            return_error!(table_not_found(plan.table.clone(), &schema.name, &plan.table.as_ref(),));
        };

        // Get the column
        let Some(column) = Catalog::get_column_by_name(atx, table.id, plan.column.as_ref())? else {
            return_error!(column_not_found(plan.column.clone()));
        };

        // Check if the column has auto_increment enabled
        if !column.auto_increment {
            return_error!(reifydb_core::diagnostic::Diagnostic {
                code: "ALTER_001".to_string(),
                statement: None,
                message: format!(
                    "cannot alter sequence for column `{}` which does not have AUTO INCREMENT",
                    column.name
                ),
                span: Some(plan.column.clone()),
                label: Some("column does not have AUTO INCREMENT".to_string()),
                help: Some(
                    "only columns with AUTO INCREMENT can have their sequences altered".to_string()
                ),
                column: None,
                notes: vec![],
                cause: None,
            });
        }

        // Convert the value to u64
        let value_u64 = if plan.value < 0 {
            return_error!(reifydb_core::diagnostic::Diagnostic {
                code: "ALTER_002".to_string(),
                statement: None,
                message: format!("sequence value cannot be negative: {}", plan.value),
                span: None,
                label: Some("negative sequence value".to_string()),
                help: Some("sequence values must be positive integers".to_string()),
                column: None,
                notes: vec![],
                cause: None,
            });
        } else if plan.value > u64::MAX as i128 {
            return_error!(reifydb_core::diagnostic::Diagnostic {
                code: "ALTER_003".to_string(),
                statement: None,
                message: format!("sequence value too large: {}", plan.value),
                span: None,
                label: Some("value exceeds maximum".to_string()),
                help: Some(format!("sequence values must not exceed {}", u64::MAX)),
                column: None,
                notes: vec![],
                cause: None,
            });
        } else {
            plan.value as u64
        };

        // Get the sequence key
        let key = TableColumnSequenceKey { table: table.id, column: column.id }.encode();

        // Set the sequence value
        atx.with_unversioned_write(|tx| {
            use once_cell::sync::Lazy;
            use reifydb_core::row::EncodedRowLayout;

            static LAYOUT: Lazy<EncodedRowLayout> =
                Lazy::new(|| EncodedRowLayout::new(&[Type::Uint8]));

            let mut row = LAYOUT.allocate_row();
            // The next call to next() will return value_u64, so we store value_u64 + 1
            LAYOUT.set_u64(&mut row, 0, value_u64.saturating_add(1));
            tx.set(&key, row)?;
            Ok(())
        })?;

        Ok(Columns::single_row([
            ("schema", Value::Utf8(schema.name.clone())),
            ("table", Value::Utf8(table.name.clone())),
            ("column", Value::Utf8(column.name.clone())),
            ("value", Value::Uint8(value_u64)),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use crate::execute_tx;
    use reifydb_catalog::Catalog;
    use reifydb_catalog::table::{ColumnToCreate, TableToCreate};
    use reifydb_catalog::test_utils::ensure_test_schema;
    use reifydb_core::{OwnedSpan, Type, Value};
    use reifydb_rql::plan::physical::{AlterSequencePlan, PhysicalPlan};
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_ok() {
        let mut atx = create_test_write_transaction();
        ensure_test_schema(&mut atx);

        // Create a table with an auto-increment column
        Catalog::create_table(
            &mut atx,
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
            value: 1000,
        };

        let result = execute_tx(&mut atx, PhysicalPlan::AlterSequence(plan)).unwrap();
        assert_eq!(result.row(0)[0], Value::Utf8("test_schema".to_string()));
        assert_eq!(result.row(0)[1], Value::Utf8("users".to_string()));
        assert_eq!(result.row(0)[2], Value::Utf8("id".to_string()));
        assert_eq!(result.row(0)[3], Value::Uint8(1000));
    }

    #[test]
    fn test_non_auto_increment_column() {
        let mut atx = create_test_write_transaction();
        ensure_test_schema(&mut atx);

        // Create a table with a non-auto-increment column
        Catalog::create_table(
            &mut atx,
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
            value: 100,
        };

        let err = execute_tx(&mut atx, PhysicalPlan::AlterSequence(plan)).unwrap_err();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "ALTER_001");
    }

    #[test]
    fn test_schema_not_found() {
        let mut atx = create_test_write_transaction();
        // Note: We're not creating any schema, so any schema reference should fail

        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("non_existent_schema")),
            table: OwnedSpan::testing("some_table"),
            column: OwnedSpan::testing("id"),
            value: 100,
        };

        let err = execute_tx(&mut atx, PhysicalPlan::AlterSequence(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_002");
    }

    #[test]
    fn test_table_not_found() {
        let mut atx = create_test_write_transaction();
        ensure_test_schema(&mut atx);

        let plan = AlterSequencePlan {
            schema: Some(OwnedSpan::testing("test_schema")),
            table: OwnedSpan::testing("non_existent_table"),
            column: OwnedSpan::testing("id"),
            value: 100,
        };

        let err = execute_tx(&mut atx, PhysicalPlan::AlterSequence(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_004");
    }

    #[test]
    fn test_column_not_found() {
        let mut atx = create_test_write_transaction();
        ensure_test_schema(&mut atx);

        // Create a table
        Catalog::create_table(
            &mut atx,
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
            value: 100,
        };

        let err = execute_tx(&mut atx, PhysicalPlan::AlterSequence(plan)).unwrap_err();
        assert_eq!(err.diagnostic().code, "QUERY_001");
    }
}
