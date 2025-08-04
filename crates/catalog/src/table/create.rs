// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::ColumnIndex;
use crate::column_policy::ColumnPolicyKind;
use crate::schema::SchemaId;
use crate::sequence::SystemSequence;
use crate::table::layout::{table, table_schema};
use reifydb_core::interface::{
    ActiveWriteTransaction, EncodableKey, Key, SchemaTableKey, Table, TableId, TableKey,
    UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::interface::{VersionedWriteTransaction};
use reifydb_core::result::error::diagnostic::catalog::{schema_not_found, table_already_exists};
use reifydb_core::{OwnedSpan, Type, return_error};

#[derive(Debug, Clone)]
pub struct ColumnToCreate {
    pub name: String,
    pub ty: Type,
    pub policies: Vec<ColumnPolicyKind>,
    pub auto_increment: bool,
    pub span: Option<OwnedSpan>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
    pub span: Option<OwnedSpan>,
    pub table: String,
    pub schema: String,
    pub columns: Vec<ColumnToCreate>,
}

impl Catalog {
    pub fn create_table<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        to_create: TableToCreate,
    ) -> crate::Result<Table> {
        let Some(schema) = Catalog::get_schema_by_name(atx, &to_create.schema)? else {
            return_error!(schema_not_found(to_create.span, &to_create.schema));
        };

        if let Some(table) = Catalog::get_table_by_name(atx, schema.id, &to_create.table)? {
            return_error!(table_already_exists(to_create.span, &schema.name, &table.name));
        }

        let table_id = SystemSequence::next_table_id(atx)?;
        Self::store_table(atx, table_id, schema.id, &to_create)?;
        Self::link_table_to_schema(atx, schema.id, table_id, &to_create.table)?;

        Catalog::insert_columns(atx, table_id, to_create)?;

        Ok(Catalog::get_table(atx, table_id)?.unwrap())
    }

    fn store_table<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        table: TableId,
        schema: SchemaId,
        to_create: &TableToCreate,
    ) -> crate::Result<()> {
        let mut row = table::LAYOUT.allocate_row();
        table::LAYOUT.set_u64(&mut row, table::ID, table);
        table::LAYOUT.set_u64(&mut row, table::SCHEMA, schema);
        table::LAYOUT.set_utf8(&mut row, table::NAME, &to_create.table);

        atx.set(&TableKey { table }.encode(), row)?;

        Ok(())
    }

    fn link_table_to_schema<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        schema: SchemaId,
        table: TableId,
        name: &str,
    ) -> crate::Result<()> {
        let mut row = table_schema::LAYOUT.allocate_row();
        table_schema::LAYOUT.set_u64(&mut row, table_schema::ID, table);
        table_schema::LAYOUT.set_utf8(&mut row, table_schema::NAME, name);
        atx.set(&Key::SchemaTable(SchemaTableKey { schema, table }).encode(), row)?;
        Ok(())
    }

    fn insert_columns<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        table: TableId,
        to_create: TableToCreate,
    ) -> crate::Result<()> {
        for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
            Catalog::create_column(
                atx,
                table,
                crate::column::ColumnToCreate {
                    span: column_to_create.span.clone(),
                    schema_name: &to_create.schema,
                    table,
                    table_name: &to_create.table,
                    column: column_to_create.name,
                    value: column_to_create.ty,
                    if_not_exists: false,
                    policies: column_to_create.policies.clone(),
                    index: ColumnIndex(idx as u16),
                    auto_increment: column_to_create.auto_increment,
                },
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::schema::SchemaId;
    use crate::table::TableToCreate;
    use crate::table::layout::table_schema;
    use crate::test_utils::ensure_test_schema;
    use reifydb_core::interface::SchemaTableKey;
    use reifydb_core::interface::VersionedReadTransaction;
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_create_table() {
        let mut atx = create_test_write_transaction();

        ensure_test_schema(&mut atx);

        let to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            columns: vec![],
            span: None,
        };

        // First creation should succeed
        let result = Catalog::create_table(&mut atx, to_create.clone()).unwrap();
        assert_eq!(result.id, 1);
        assert_eq!(result.schema, 1);
        assert_eq!(result.name, "test_table");

        // Creating the same table again with `if_not_exists = false` should return error
        let err = Catalog::create_table(&mut atx, to_create).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_003");
    }

    #[test]
    fn test_table_linked_to_schema() {
        let mut atx = create_test_write_transaction();
        ensure_test_schema(&mut atx);

        let to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            columns: vec![],
            span: None,
        };

        Catalog::create_table(&mut atx, to_create).unwrap();

        let to_create = TableToCreate {
            schema: "test_schema".to_string(),
            table: "another_table".to_string(),
            columns: vec![],
            span: None,
        };

        Catalog::create_table(&mut atx, to_create).unwrap();

        let links = atx.range(SchemaTableKey::full_scan(SchemaId(1))).unwrap().collect::<Vec<_>>();
        assert_eq!(links.len(), 2);

        let link = &links[1];
        let row = &link.row;
        assert_eq!(table_schema::LAYOUT.get_u64(row, table_schema::ID), 1);
        assert_eq!(table_schema::LAYOUT.get_utf8(row, table_schema::NAME), "test_table");

        let link = &links[0];
        let row = &link.row;
        assert_eq!(table_schema::LAYOUT.get_u64(row, table_schema::ID), 2);
        assert_eq!(table_schema::LAYOUT.get_utf8(row, table_schema::NAME), "another_table");
    }

    #[test]
    fn test_create_table_missing_schema() {
        let mut atx = create_test_write_transaction();

        let to_create = TableToCreate {
            schema: "missing_schema".to_string(),
            table: "my_table".to_string(),
            columns: vec![],
            span: None,
        };

        let err = Catalog::create_table(&mut atx, to_create).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_002");
    }
}
