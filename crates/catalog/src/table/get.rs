// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::schema::SchemaId;
use crate::table::layout::{table, table_schema};
use reifydb_core::interface::{EncodableKey, SchemaTableKey, Table, TableId, TableKey};
use reifydb_core::interface::{Versioned, VersionedQueryTransaction};

impl Catalog {
    pub fn get_table_by_name(
		rx: &mut impl VersionedQueryTransaction,
		schema: SchemaId,
		name: impl AsRef<str>,
    ) -> crate::Result<Option<Table>> {
        let name = name.as_ref();
        let Some(table) =
            rx.range(SchemaTableKey::full_scan(schema))?.find_map(|versioned: Versioned| {
                let row = &versioned.row;
                let table_name = table_schema::LAYOUT.get_utf8(row, table_schema::NAME);
                if name == table_name {
                    Some(TableId(table_schema::LAYOUT.get_u64(row, table_schema::ID)))
                } else {
                    None
                }
            })
        else {
            return Ok(None);
        };

        Catalog::get_table(rx, table)
    }

    pub fn get_table(
		rx: &mut impl VersionedQueryTransaction,
		table: TableId,
    ) -> crate::Result<Option<Table>> {
        match rx.get(&TableKey { table }.encode())? {
            Some(versioned) => {
                let row = versioned.row;
                let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
                let schema = SchemaId(table::LAYOUT.get_u64(&row, table::SCHEMA));
                let name = table::LAYOUT.get_utf8(&row, table::NAME).to_string();
                Ok(Some(Table { id, name, schema, columns: Catalog::list_columns(rx, id)? }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    mod get_table_by_name {
        use crate::Catalog;
        use crate::schema::SchemaId;
        use crate::test_utils::{create_schema, create_table, ensure_test_schema};
        use reifydb_transaction::test_utils::create_test_command_transaction;

        #[test]
        fn test_ok() {
            let mut txn = create_test_command_transaction();
            ensure_test_schema(&mut txn);
            create_schema(&mut txn, "schema_one");
            create_schema(&mut txn, "schema_two");
            create_schema(&mut txn, "schema_three");

            create_table(&mut txn, "schema_one", "table_one", &[]);
            create_table(&mut txn, "schema_two", "table_two", &[]);
            create_table(&mut txn, "schema_three", "table_three", &[]);

            let result =
                Catalog::get_table_by_name(&mut txn, SchemaId(3), "table_two").unwrap().unwrap();
            assert_eq!(result.id, 2);
            assert_eq!(result.schema, 3);
            assert_eq!(result.name, "table_two");
        }

        #[test]
        fn test_empty() {
            let mut txn = create_test_command_transaction();
            let result = Catalog::get_table_by_name(&mut txn, SchemaId(1), "some_table").unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn test_not_found_different_table() {
            let mut txn = create_test_command_transaction();
            ensure_test_schema(&mut txn);
            create_schema(&mut txn, "schema_one");
            create_schema(&mut txn, "schema_two");
            create_schema(&mut txn, "schema_three");

            create_table(&mut txn, "schema_one", "table_one", &[]);
            create_table(&mut txn, "schema_two", "table_two", &[]);
            create_table(&mut txn, "schema_three", "table_three", &[]);

            let result =
                Catalog::get_table_by_name(&mut txn, SchemaId(1), "table_four_two").unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn test_not_found_different_schema() {
            let mut txn = create_test_command_transaction();
            ensure_test_schema(&mut txn);
            create_schema(&mut txn, "schema_one");
            create_schema(&mut txn, "schema_two");
            create_schema(&mut txn, "schema_three");

            create_table(&mut txn, "schema_one", "table_one", &[]);
            create_table(&mut txn, "schema_two", "table_two", &[]);
            create_table(&mut txn, "schema_three", "table_three", &[]);

            let result = Catalog::get_table_by_name(&mut txn, SchemaId(2), "table_two").unwrap();
            assert!(result.is_none());
        }
    }

    mod get_table {
        use crate::Catalog;
        use crate::test_utils::{create_schema, create_table, ensure_test_schema};
        use reifydb_core::interface::TableId;
        use reifydb_transaction::test_utils::create_test_command_transaction;

        #[test]
        fn test_ok() {
            let mut txn = create_test_command_transaction();
            ensure_test_schema(&mut txn);
            create_schema(&mut txn, "schema_one");
            create_schema(&mut txn, "schema_two");
            create_schema(&mut txn, "schema_three");

            create_table(&mut txn, "schema_one", "table_one", &[]);
            create_table(&mut txn, "schema_two", "table_two", &[]);
            create_table(&mut txn, "schema_three", "table_three", &[]);

            let result = Catalog::get_table(&mut txn, TableId(2)).unwrap().unwrap();
            assert_eq!(result.id, 2);
            assert_eq!(result.schema, 3);
            assert_eq!(result.name, "table_two");
        }

        #[test]
        fn test_not_found() {
            let mut txn = create_test_command_transaction();
            ensure_test_schema(&mut txn);
            create_schema(&mut txn, "schema_one");
            create_schema(&mut txn, "schema_two");
            create_schema(&mut txn, "schema_three");

            create_table(&mut txn, "schema_one", "table_one", &[]);
            create_table(&mut txn, "schema_two", "table_two", &[]);
            create_table(&mut txn, "schema_three", "table_three", &[]);

            let result = Catalog::get_table(&mut txn, TableId(42)).unwrap();
            assert!(result.is_none());
        }
    }
}
