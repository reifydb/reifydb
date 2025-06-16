// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::catalog::layout::{column, table_column};
use reifydb_core::catalog::{Column, ColumnId, TableId};
use reifydb_core::{ColumnKey, EncodableKey, TableColumnKey, ValueKind};
use reifydb_storage::{UnversionedStorage, Versioned, VersionedStorage};
use reifydb_transaction::Rx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn get_column(
        &mut self,
        rx: &mut impl Rx,
        column: ColumnId,
    ) -> crate::Result<Option<Column>> {
        Ok(rx.get(&ColumnKey { column }.encode())?.map(Self::convert_column))
    }

    pub(crate) fn get_column_by_name(
        &mut self,
        rx: &mut impl Rx,
        table: TableId,
        column_name: &str,
    ) -> crate::Result<Option<Column>> {
        let maybe_id = rx.scan_range(TableColumnKey::full_scan(table))?.find_map(|versioned| {
            let row = versioned.row;
            let column = ColumnId(table_column::LAYOUT.get_u32(&row, table_column::ID));
            let name = table_column::LAYOUT.get_str(&row, table_column::NAME);

            if name == column_name { Some(column) } else { None }
        });
        
        if let Some(id) = maybe_id { self.get_column(rx, id) } else { Ok(None) }
    }

    fn convert_column(versioned: Versioned) -> Column {
        let row = versioned.row;

        let id = ColumnId(column::LAYOUT.get_u32(&row, column::ID));
        let name = column::LAYOUT.get_str(&row, column::NAME).to_string();
        let value = ValueKind::from_u8(column::LAYOUT.get_u8(&row, column::VALUE));

        Column { id, name, value }
    }
}

#[cfg(test)]
mod tests {
    use crate::execute::Executor;
    use crate::test_utils::create_test_table_column;
    use reifydb_core::ValueKind;
    use reifydb_core::catalog::{ColumnId, TableId};
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_get_column() {
        let mut tx = TestTransaction::new();
        create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
        create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
        create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

        let mut executor = Executor::testing();
        let result = executor.get_column(&mut tx, ColumnId(2)).unwrap().unwrap();

        assert_eq!(result.id, 2);
        assert_eq!(result.name, "col_2");
        assert_eq!(result.value, ValueKind::Int2);
    }

    #[test]
    fn test_get_column_not_found() {
        let mut tx = TestTransaction::new();
        create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
        create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
        create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

        let mut executor = Executor::testing();
        let result = executor.get_column(&mut tx, ColumnId(4)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_column_by_name() {
        let mut tx = TestTransaction::new();
        create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);
        create_test_table_column(&mut tx, "col_2", ValueKind::Int2, vec![]);
        create_test_table_column(&mut tx, "col_3", ValueKind::Int4, vec![]);

        let mut executor = Executor::testing();
        let result = executor.get_column_by_name(&mut tx, TableId(1), "col_3").unwrap().unwrap();

        assert_eq!(result.id, 3);
        assert_eq!(result.name, "col_3");
        assert_eq!(result.value, ValueKind::Int4);
    }

    #[test]
    fn test_get_table_column_by_name_not_found() {
        let mut tx = TestTransaction::new();
        create_test_table_column(&mut tx, "col_1", ValueKind::Int1, vec![]);

        let mut executor = Executor::testing();
        let result = executor.get_column_by_name(&mut tx, TableId(1), "not_found").unwrap();

        assert!(result.is_none());
    }
}
