// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::layout::table_column;
use crate::column::{Column, ColumnId};
use crate::key::TableColumnKey;
use crate::table::TableId;
use reifydb_core::interface::Rx;

impl Catalog {
    pub fn list_columns(rx: &mut impl Rx, table: TableId) -> crate::Result<Vec<Column>> {
        let mut result = vec![];

        let ids = rx
            .scan_range(TableColumnKey::full_scan(table))?
            .map(|versioned| {
                let row = versioned.row;
                ColumnId(table_column::LAYOUT.get_u64(&row, table_column::ID))
            })
            .collect::<Vec<_>>();

        for id in ids {
            result.push(Catalog::get_column(rx, id)?.unwrap());
        }

        result.sort_by_key(|c| c.index);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::column::{ColumnIndex, ColumnToCreate};
    use crate::table::TableId;
    use crate::test_utils::ensure_test_table;
    use reifydb_core::DataType;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_ok() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);

        // Create columns out of order
        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "b_col".to_string(),
                value: DataType::Int4,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
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
                column: "a_col".to_string(),
                value: DataType::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        let columns = Catalog::list_columns(&mut tx, TableId(1)).unwrap();
        assert_eq!(columns.len(), 2);

        assert_eq!(columns[0].name, "a_col"); // index 0
        assert_eq!(columns[1].name, "b_col"); // index 1

        assert_eq!(columns[0].index, ColumnIndex(0));
        assert_eq!(columns[1].index, ColumnIndex(1));
    }

    #[test]
    fn test_empty() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);
        let columns = Catalog::list_columns(&mut tx, TableId(1)).unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_table_does_not_exist() {
        let mut tx = TestTransaction::new();
        let columns = Catalog::list_columns(&mut tx, TableId(1)).unwrap();
        assert!(columns.is_empty());
    }
}
