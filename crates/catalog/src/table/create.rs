// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::column::ColumnPolicy;
use crate::table::Table;
use crate::table::layout::{table, table_schema};
use crate::{Catalog, Error};
use reifydb_core::catalog::{SchemaId, TableId};
use reifydb_core::{Key, SchemaTableKey, TableKey, ValueKind};
use reifydb_diagnostic::{Diagnostic, Span};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

#[derive(Debug, Clone)]
pub struct ColumnToCreate {
    pub name: String,
    pub value: ValueKind,
    pub policies: Vec<ColumnPolicy>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
    pub span: Option<Span>,
    pub table: String,
    pub schema: String,
    pub columns: Vec<ColumnToCreate>,
}

impl Catalog {
    pub fn create_table<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        to_create: TableToCreate,
    ) -> crate::Result<Table> {
        let Some(schema) = Catalog::get_schema_by_name(tx, &to_create.schema)? else {
            return Err(Error(Diagnostic::schema_not_found(to_create.span, &to_create.schema)));
        };

        if let Some(table) = Catalog::get_table_by_name(tx, &to_create.table)? {
            return Err(Error(Diagnostic::table_already_exists(
                to_create.span,
                &schema.name,
                &table.name,
            )));
        }

        let table_id = Catalog::next_table_id(tx)?;
        Self::store_table(tx, table_id, schema.id, &to_create)?;
        Self::link_table_to_schema(tx, table_id, schema.id)?;

        Catalog::insert_columns(tx, table_id, to_create)?;

        Ok(Catalog::get_table(tx, table_id)?.unwrap())
    }

    fn store_table<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
        schema: SchemaId,
        to_create: &TableToCreate,
    ) -> crate::Result<()> {
        let mut row = table::LAYOUT.allocate_row();
        table::LAYOUT.set_u32(&mut row, table::ID, table);
        table::LAYOUT.set_u32(&mut row, table::SCHEMA, schema);
        table::LAYOUT.set_str(&mut row, table::NAME, &to_create.table);

        tx.set(&Key::Table(TableKey { table }).encode(), row)?;

        Ok(())
    }

    fn link_table_to_schema<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
        schema: SchemaId,
    ) -> crate::Result<()> {
        let mut row = table_schema::LAYOUT.allocate_row();
        table_schema::LAYOUT.set_u32(&mut row, table_schema::ID, table);
        tx.set(&Key::SchemaTable(SchemaTableKey { schema, table }).encode(), row)?;
        Ok(())
    }

    fn insert_columns<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
        to_create: TableToCreate,
    ) -> crate::Result<()> {
        for column_to_create in to_create.columns {
            Catalog::create_column(
                tx,
                table,
                crate::column::ColumnToCreate {
                    span: None,
                    schema_name: &to_create.schema,
                    table,
                    table_name: &to_create.table,
                    column: column_to_create.name,
                    value: column_to_create.value,
                    if_not_exists: false,
                    policies: column_to_create.policies.clone(),
                },
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::table::TableToCreate;
    use crate::test_utils::ensure_test_schema;
    use reifydb_core::catalog::{SchemaId, TableId};
    use reifydb_core::row::EncodedRow;
    use reifydb_core::{AsyncCowVec, EncodableKey, SchemaTableKey};
    use reifydb_transaction::Rx;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_create_table() {
        let mut tx = TestTransaction::new();

        ensure_test_schema(&mut tx);

        let mut to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            columns: vec![],
            span: None,
        };

        // First creation should succeed
        let result = Catalog::create_table(&mut tx, to_create.clone()).unwrap();
        assert_eq!(result.id, 1);
        assert_eq!(result.schema, 1);
        assert_eq!(result.name, "test_table");

        // Creating the same table again with `if_not_exists = false` should return error
        let err = Catalog::create_table(&mut tx, to_create).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_003");
    }

    #[test]
    fn test_table_linked_to_schema() {
        let mut tx = TestTransaction::new();
        ensure_test_schema(&mut tx);

        let to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            columns: vec![],
            span: None,
        };

        Catalog::create_table(&mut tx, to_create).unwrap();

        let to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "another_table".to_string(),
            columns: vec![],
            span: None,
        };

        Catalog::create_table(&mut tx, to_create).unwrap();

        let links =
            tx.scan_range(SchemaTableKey::full_scan(SchemaId(1))).unwrap().collect::<Vec<_>>();
        assert_eq!(links.len(), 2);

        let link = &links[0];
        assert_eq!(link.key, SchemaTableKey { schema: SchemaId(1), table: TableId(1) }.encode());

        assert_eq!(
            link.row,
            EncodedRow(AsyncCowVec::new([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00].to_vec())) // validity + padding + id
        );

        let link = &links[1];
        assert_eq!(link.key, SchemaTableKey { schema: SchemaId(1), table: TableId(2) }.encode());

        assert_eq!(
            link.row,
            EncodedRow(AsyncCowVec::new([0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00].to_vec())) // validity + padding + id
        )
    }

    #[test]
    fn test_create_table_missing_schema() {
        let mut tx = TestTransaction::new();

        let to_create = TableToCreate {
            schema: "missing_schema".to_string(),
            table: "my_table".to_string(),
            columns: vec![],
            span: None,
        };

        let err = Catalog::create_table(&mut tx, to_create).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_002");
    }
}
