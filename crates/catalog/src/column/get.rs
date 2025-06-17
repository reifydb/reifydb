// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Catalog;
use crate::column::layout::{column, table_column};
use crate::column::{Column, ColumnId, ColumnIndex};
use crate::key::{ColumnKey, EncodableKey, TableColumnKey};
use crate::table::TableId;
use reifydb_core::ValueKind;
use reifydb_storage::Versioned;
use reifydb_transaction::Rx;

impl Catalog {
    pub fn get_column(rx: &mut impl Rx, column: ColumnId) -> crate::Result<Option<Column>> {
        Ok(rx.get(&ColumnKey { column }.encode())?.map(Self::convert_column))
    }

    pub fn get_column_by_name(
        rx: &mut impl Rx,
        table: TableId,
        column_name: &str,
    ) -> crate::Result<Option<Column>> {
        let maybe_id = rx.scan_range(TableColumnKey::full_scan(table))?.find_map(|versioned| {
            let row = versioned.row;
            let column = ColumnId(table_column::LAYOUT.get_u64(&row, table_column::ID));
            let name = table_column::LAYOUT.get_str(&row, table_column::NAME);

            if name == column_name { Some(column) } else { None }
        });

        if let Some(id) = maybe_id { Catalog::get_column(rx, id) } else { Ok(None) }
    }

    fn convert_column(versioned: Versioned) -> Column {
        let row = versioned.row;

        let id = ColumnId(column::LAYOUT.get_u64(&row, column::ID));
        let name = column::LAYOUT.get_str(&row, column::NAME).to_string();
        let value = ValueKind::from_u8(column::LAYOUT.get_u8(&row, column::VALUE));
        let index = ColumnIndex(column::LAYOUT.get_u16(&row, column::INDEX));

        Column { id, name, value, index, policies: vec![] }
    }
}

#[cfg(test)]
mod tests {
    mod get_column {
        use crate::Catalog;
        use crate::column::ColumnId;
        use crate::test_utils::create_test_table_column;
        use reifydb_core::ValueKind;
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
            create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
            create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

            let result = Catalog::get_column(&mut tx, ColumnId(2)).unwrap().unwrap();

            assert_eq!(result.id, 2);
            assert_eq!(result.name, "col_2");
            assert_eq!(result.value, ValueKind::Int2);
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
            create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
            create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

            let result = Catalog::get_column(&mut tx, ColumnId(4)).unwrap();
            assert!(result.is_none());
        }
    }

    mod get_column_by_name {
        use crate::Catalog;
        use crate::table::TableId;
        use crate::test_utils::create_test_table_column;
        use reifydb_core::ValueKind;
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
            create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
            create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

            let result =
                Catalog::get_column_by_name(&mut tx, TableId(1), "col_3").unwrap().unwrap();

            assert_eq!(result.id, 3);
            assert_eq!(result.name, "col_3");
            assert_eq!(result.value, ValueKind::Int4);
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);

            let result = Catalog::get_column_by_name(&mut tx, TableId(1), "not_found").unwrap();

            assert!(result.is_none());
        }
    }
}
