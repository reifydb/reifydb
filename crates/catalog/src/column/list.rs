// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::layout::table_column;
use crate::column::{Column, ColumnId};
use reifydb_core::interface::VersionedReadTransaction;
use reifydb_core::interface::{TableColumnKey, TableId};

impl Catalog {
    pub fn list_columns(
        rx: &mut impl VersionedReadTransaction,
        table: TableId,
    ) -> crate::Result<Vec<Column>> {
        let mut result = vec![];

        let ids = rx
            .range(TableColumnKey::full_scan(table))?
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
    use crate::test_utils::ensure_test_table;
    use reifydb_core::Type;
    use reifydb_core::interface::TableId;
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_ok() {
        let mut atx = create_test_write_transaction();
        ensure_test_table(&mut atx);

        // Create columns out of order
        Catalog::create_column(
            &mut atx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "b_col".to_string(),
                value: Type::Int4,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(1),
            },
        )
        .unwrap();

        Catalog::create_column(
            &mut atx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "a_col".to_string(),
                value: Type::Bool,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        let columns = Catalog::list_columns(&mut atx, TableId(1)).unwrap();
        assert_eq!(columns.len(), 2);

        assert_eq!(columns[0].name, "a_col"); // index 0
        assert_eq!(columns[1].name, "b_col"); // index 1

        assert_eq!(columns[0].index, ColumnIndex(0));
        assert_eq!(columns[1].index, ColumnIndex(1));
    }

    #[test]
    fn test_empty() {
        let mut atx = create_test_write_transaction();
        ensure_test_table(&mut atx);
        let columns = Catalog::list_columns(&mut atx, TableId(1)).unwrap();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_table_does_not_exist() {
        let mut atx = create_test_write_transaction();
        let columns = Catalog::list_columns(&mut atx, TableId(1)).unwrap();
        assert!(columns.is_empty());
    }
}
