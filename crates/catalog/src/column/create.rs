// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::column::layout::{column, table_column};
use crate::column::{Column, ColumnIndex, ColumnPolicyKind};
use crate::key::{ColumnKey, EncodableKey, Key, TableColumnKey};
use crate::sequence::SystemSequence;
use crate::table::TableId;
use crate::{Catalog, Error};
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_core::{DataType, Span};
use reifydb_diagnostic::catalog::column_already_exists;

pub struct ColumnToCreate<'a> {
    pub span: Option<Span>,
    pub schema_name: &'a str,
    pub table: TableId,
    pub table_name: &'a str,
    pub column: String,
    pub value: DataType,
    pub if_not_exists: bool,
    pub policies: Vec<ColumnPolicyKind>,
    pub index: ColumnIndex,
}

impl Catalog {
    pub(crate) fn create_column<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
        column_to_create: ColumnToCreate,
    ) -> crate::Result<Column> {
        // FIXME policies
        if let Some(column) = Catalog::get_column_by_name(tx, table, &column_to_create.column)? {
            return Err(Error(column_already_exists(
                None,
                column_to_create.schema_name,
                column_to_create.table_name,
                &column.name,
            )));
        }

        let id = SystemSequence::next_column_id(tx)?;

        let mut row = column::LAYOUT.allocate_row();
        column::LAYOUT.set_u64(&mut row, column::ID, id);
        column::LAYOUT.set_u64(&mut row, column::TABLE, table);
        column::LAYOUT.set_str(&mut row, column::NAME, &column_to_create.column);
        column::LAYOUT.set_u8(&mut row, column::VALUE, column_to_create.value.to_u8());
        column::LAYOUT.set_u16(&mut row, column::INDEX, column_to_create.index);

        tx.set(&Key::Column(ColumnKey { column: id }).encode(), row)?;

        let mut row = table_column::LAYOUT.allocate_row();
        table_column::LAYOUT.set_u64(&mut row, table_column::ID, id);
        table_column::LAYOUT.set_str(&mut row, table_column::NAME, &column_to_create.column);
        table_column::LAYOUT.set_u16(&mut row, table_column::INDEX, column_to_create.index);
        tx.set(&TableColumnKey { table, column: id }.encode(), row)?;

        for policy in column_to_create.policies {
            Catalog::create_column_policy(tx, id, policy)?;
        }

        Ok(Column {
            id,
            name: column_to_create.column,
            value: column_to_create.value,
            index: column_to_create.index,
            policies: Catalog::list_column_policies(tx, id)?,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::Catalog;
    use crate::column::{ColumnId, ColumnIndex, ColumnToCreate};
    use crate::table::TableId;
    use crate::test_utils::ensure_test_table;
    use reifydb_core::DataType;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_create_column() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);

        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: DataType::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_2".to_string(),
                value: DataType::Int2,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
            },
        )
        .unwrap();

        let column_1 = Catalog::get_column(&mut tx, ColumnId(1)).unwrap().unwrap();
        assert_eq!(column_1.id, 1);
        assert_eq!(column_1.name, "col_1");
        assert_eq!(column_1.value, DataType::Bool);

        let column_2 = Catalog::get_column(&mut tx, ColumnId(2)).unwrap().unwrap();
        assert_eq!(column_2.id, 2);
        assert_eq!(column_2.name, "col_2");
        assert_eq!(column_2.value, DataType::Int2);
    }

    #[test]
    fn test_column_already_exists() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);

        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: DataType::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        // Tries to create a column with the same name again
        let err = Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "col_1".to_string(),
                value: DataType::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
            },
        )
        .unwrap_err();

        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "CA_005");
    }
}
