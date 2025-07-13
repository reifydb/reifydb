// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::key::{EncodableKey, SchemaKey};
use crate::schema::layout::schema;
use crate::schema::{Schema, SchemaId};
use reifydb_core::interface::{Rx, Versioned};
use reifydb_core::row::EncodedRow;

impl Catalog {
    pub fn get_schema_by_name(
        rx: &mut impl Rx,
        name: impl AsRef<str>,
    ) -> crate::Result<Option<Schema>> {
        let name = name.as_ref();
        Ok(rx.scan_range(SchemaKey::full_scan())?.find_map(|versioned| {
            let row: &EncodedRow = &versioned.row;
            let schema_name = schema::LAYOUT.get_utf8(row, schema::NAME);
            if name == schema_name { Some(Self::convert_schema(versioned)) } else { None }
        }))
    }

    pub fn get_schema(rx: &mut impl Rx, schema: SchemaId) -> crate::Result<Option<Schema>> {
        Ok(rx.get(&SchemaKey { schema }.encode())?.map(Self::convert_schema))
    }

    fn convert_schema(versioned: Versioned) -> Schema {
        let row = versioned.row;
        let id = SchemaId(schema::LAYOUT.get_u64(&row, schema::ID));
        let name = schema::LAYOUT.get_utf8(&row, schema::NAME).to_string();

        Schema { id, name }
    }
}

#[cfg(test)]
mod tests {

    mod get_schema_by_name {
        use crate::Catalog;
        use crate::test_utils::create_schema;
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            create_schema(&mut tx, "test_schema");

            let schema = Catalog::get_schema_by_name(&mut tx, "test_schema").unwrap().unwrap();

            assert_eq!(schema.id, 1);
            assert_eq!(schema.name, "test_schema");
        }

        #[test]
        fn test_empty() {
            let mut tx = TestTransaction::new();
            let result = Catalog::get_schema_by_name(&mut tx, "test_schema").unwrap();

            assert_eq!(result, None);
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            create_schema(&mut tx, "another_schema");

            let result = Catalog::get_schema_by_name(&mut tx, "test_schema").unwrap();
            assert_eq!(result, None);
        }
    }

    mod get_schema {
        use crate::Catalog;
        use crate::schema::SchemaId;
        use crate::test_utils::create_schema;
        use reifydb_transaction::test_utils::TestTransaction;

        #[test]
        fn test_ok() {
            let mut tx = TestTransaction::new();
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            let result = Catalog::get_schema(&mut tx, SchemaId(2)).unwrap().unwrap();
            assert_eq!(result.id, 2);
            assert_eq!(result.name, "schema_two");
        }

        #[test]
        fn test_not_found() {
            let mut tx = TestTransaction::new();
            create_schema(&mut tx, "schema_one");
            create_schema(&mut tx, "schema_two");
            create_schema(&mut tx, "schema_three");

            let result = Catalog::get_schema(&mut tx, SchemaId(23)).unwrap();
            assert!(result.is_none());
        }
    }
}
