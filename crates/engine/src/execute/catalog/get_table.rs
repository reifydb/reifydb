// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::catalog::layout::table;
use reifydb_core::catalog::{SchemaId, Table, TableId};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodableKey, TableKey};
use reifydb_storage::{UnversionedStorage, Versioned, VersionedStorage};
use reifydb_transaction::Rx;

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn get_table_by_name(
        &mut self,
        rx: &mut impl Rx,
        name: &str,
    ) -> crate::Result<Option<Table>> {
        Ok(rx.scan_range(TableKey::full_scan())?.find_map(|versioned| {
            let row: &EncodedRow = &versioned.row;
            let table_name = table::LAYOUT.get_str(row, table::NAME);
            if name == table_name { Some(Self::convert_table(versioned)) } else { None }
        }))
    }

    pub(crate) fn get_table(
        &mut self,
        rx: &mut impl Rx,
        table: TableId,
    ) -> crate::Result<Option<Table>> {
        Ok(rx.get(&TableKey { table }.encode())?.map(Self::convert_table))
    }

    fn convert_table(versioned: Versioned) -> Table {
        let row = versioned.row;
        let id = TableId(table::LAYOUT.get_u32(&row, table::ID));
        let schema = SchemaId(table::LAYOUT.get_u32(&row, table::SCHEMA));
        let name = table::LAYOUT.get_str(&row, table::NAME).to_string();
        Table { id, name, schema }
    }
}

#[cfg(test)]
mod tests {

    mod get_table_by_name {
        use crate::execute::Executor;
        use crate::test_utils::{create_schema, create_table, ensure_test_schema};
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            ensure_test_schema(&mut tx);
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            create_table(&mut tx, "schema_one", "table_one", &[]);
            create_table(&mut tx, "schema_two", "table_two", &[]);
            create_table(&mut tx, "schema_three", "table_three", &[]);

            let mut executor = Executor::testing();
            let result = executor.get_table_by_name(&mut tx, "table_two").unwrap().unwrap();
            assert_eq!(result.id, 2);
            assert_eq!(result.schema, 3);
            assert_eq!(result.name, "table_two");
        }

        #[test]
        fn test_empty() {
            let mut tx = TestTransaction::new();
            let mut executor = Executor::testing();
            let result = executor.get_table_by_name(&mut tx, "some_table").unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            ensure_test_schema(&mut tx);
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            create_table(&mut tx, "schema_one", "table_one", &[]);
            create_table(&mut tx, "schema_two", "table_two", &[]);
            create_table(&mut tx, "schema_three", "table_three", &[]);

            let mut executor = Executor::testing();
            let result = executor.get_table_by_name(&mut tx, "table_four_two").unwrap();
            assert!(result.is_none());
        }
    }

    mod get_table {
        use crate::execute::Executor;
        use crate::test_utils::{create_schema, create_table, ensure_test_schema};
        use reifydb_core::catalog::TableId;
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            ensure_test_schema(&mut tx);
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            create_table(&mut tx, "schema_one", "table_one", &[]);
            create_table(&mut tx, "schema_two", "table_two", &[]);
            create_table(&mut tx, "schema_three", "table_three", &[]);

            let mut executor = Executor::testing();
            let result = executor.get_table(&mut tx, TableId(2)).unwrap().unwrap();
            assert_eq!(result.id, 2);
            assert_eq!(result.schema, 3);
            assert_eq!(result.name, "table_two");
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            ensure_test_schema(&mut tx);
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            create_table(&mut tx, "schema_one", "table_one", &[]);
            create_table(&mut tx, "schema_two", "table_two", &[]);
            create_table(&mut tx, "schema_three", "table_three", &[]);

            let mut executor = Executor::testing();
            let result = executor.get_table(&mut tx, TableId(42)).unwrap();
            assert!(result.is_none());
        }
    }
}
