// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::layout::{column, table_column};
use crate::column::{Column, ColumnIndex};
use crate::column_policy::ColumnPolicyKind;
use crate::sequence::SystemSequence;
use reifydb_core::diagnostic::catalog::{auto_increment_invalid_type, column_already_exists};
use reifydb_core::interface::{
    ActiveCommandTransaction, ColumnKey, EncodableKey, Key, TableColumnKey, UnversionedTransaction,
    VersionedTransaction,
};
use reifydb_core::interface::{
    TableId, VersionedCommandTransaction,
};
use reifydb_core::{OwnedSpan, Type, return_error};

pub struct ColumnToCreate<'a> {
    pub span: Option<OwnedSpan>,
    pub schema_name: &'a str,
    pub table: TableId,
    pub table_name: &'a str,
    pub column: String,
    pub value: Type,
    pub if_not_exists: bool,
    pub policies: Vec<ColumnPolicyKind>,
    pub index: ColumnIndex,
    pub auto_increment: bool,
}

impl Catalog {
    pub(crate) fn create_column<VT: VersionedTransaction, UT: UnversionedTransaction>(
        txn: &mut ActiveCommandTransaction<VT, UT>,
        table: TableId,
        column_to_create: ColumnToCreate,
    ) -> crate::Result<Column> {
        // FIXME policies
        if let Some(column) = Catalog::get_column_by_name(txn, table, &column_to_create.column)? {
            return_error!(column_already_exists(
                None::<OwnedSpan>,
                column_to_create.schema_name,
                column_to_create.table_name,
                &column.name,
            ));
        }

        // Validate auto_increment is only used with integer types
        if column_to_create.auto_increment {
            let is_integer_type = matches!(
                column_to_create.value,
                Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16 |
                Type::Uint1 | Type::Uint2 | Type::Uint4 | Type::Uint8 | Type::Uint16
            );
            
            if !is_integer_type {
                return_error!(auto_increment_invalid_type(
                    column_to_create.span,
                    &column_to_create.column,
                    column_to_create.value,
                ));
            }
        }

        let id = SystemSequence::next_column_id(txn)?;

        let mut row = column::LAYOUT.allocate_row();
        column::LAYOUT.set_u64(&mut row, column::ID, id);
        column::LAYOUT.set_u64(&mut row, column::TABLE, table);
        column::LAYOUT.set_utf8(&mut row, column::NAME, &column_to_create.column);
        column::LAYOUT.set_u8(&mut row, column::VALUE, column_to_create.value.to_u8());
        column::LAYOUT.set_u16(&mut row, column::INDEX, column_to_create.index);
        column::LAYOUT.set_bool(&mut row, column::AUTO_INCREMENT, column_to_create.auto_increment);

        txn.set(&Key::Column(ColumnKey { column: id }).encode(), row)?;

        let mut row = table_column::LAYOUT.allocate_row();
        table_column::LAYOUT.set_u64(&mut row, table_column::ID, id);
        table_column::LAYOUT.set_utf8(&mut row, table_column::NAME, &column_to_create.column);
        table_column::LAYOUT.set_u16(&mut row, table_column::INDEX, column_to_create.index);
        txn.set(&TableColumnKey { table, column: id }.encode(), row)?;

        for policy in column_to_create.policies {
            Catalog::create_column_policy(txn, id, policy)?;
        }

        Ok(Column {
            id,
            name: column_to_create.column,
            ty: column_to_create.value,
            index: column_to_create.index,
            policies: Catalog::list_column_policies(txn, id)?,
            auto_increment: column_to_create.auto_increment,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::Catalog;
    use crate::column::{ColumnIndex, ColumnToCreate};
    use crate::test_utils::ensure_test_table;
    use reifydb_core::Type;
    use reifydb_core::interface::{ColumnId, TableId};
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_create_column() {
        let mut txn = create_test_command_transaction();
        ensure_test_table(&mut txn);

        Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: Type::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: false,
            },
        )
        .unwrap();

        Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_2".to_string(),
                value: Type::Int2,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
                auto_increment: false,
            },
        )
        .unwrap();

        let column_1 = Catalog::get_column(&mut txn, ColumnId(1)).unwrap().unwrap();
        assert_eq!(column_1.id, 1);
        assert_eq!(column_1.name, "col_1");
        assert_eq!(column_1.ty, Type::Bool);
        assert_eq!(column_1.auto_increment, false);

        let column_2 = Catalog::get_column(&mut txn, ColumnId(2)).unwrap().unwrap();
        assert_eq!(column_2.id, 2);
        assert_eq!(column_2.name, "col_2");
        assert_eq!(column_2.ty, Type::Int2);
        assert_eq!(column_2.auto_increment, false);
    }

    #[test]
    fn test_create_column_with_auto_increment() {
        let mut txn = create_test_command_transaction();
        ensure_test_table(&mut txn);

        Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "id".to_string(),
                value: Type::Uint8,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: true,
            },
        )
        .unwrap();

        let column = Catalog::get_column(&mut txn, ColumnId(1)).unwrap().unwrap();
        assert_eq!(column.id, 1);
        assert_eq!(column.name, "id");
        assert_eq!(column.ty, Type::Uint8);
        assert_eq!(column.auto_increment, true);
    }

    #[test]
    fn test_auto_increment_invalid_type() {
        let mut txn = create_test_command_transaction();
        ensure_test_table(&mut txn);

        // Try to create a text column with auto_increment
        let err = Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "name".to_string(),
                value: Type::Utf8,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: true,
            },
        )
        .unwrap_err();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "CA_006");
        assert!(diagnostic.message.contains("auto increment is not supported for type"));

        // Try with bool type
        let err = Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "is_active".to_string(),
                value: Type::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: true,
            },
        )
        .unwrap_err();

        assert_eq!(err.diagnostic().code, "CA_006");

        // Try with float type
        let err = Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "price".to_string(),
                value: Type::Float8,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: true,
            },
        )
        .unwrap_err();

        assert_eq!(err.diagnostic().code, "CA_006");
    }

    #[test]
    fn test_column_already_exists() {
        let mut txn = create_test_command_transaction();
        ensure_test_table(&mut txn);

        Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: Type::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
                auto_increment: false,
            },
        )
        .unwrap();

        // Tries to create a column with the same name again
        let err = Catalog::create_column(
            &mut txn,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: Type::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
                auto_increment: false,
            },
        )
        .unwrap_err();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "CA_005");
    }
}
