// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{ColumnIndex, ColumnToCreate};
use crate::column_policy::ColumnPolicyKind;
use crate::schema::SchemaToCreate;
use crate::schema::{Schema, SchemaId};
use crate::table::TableToCreate;
use crate::{Catalog, table};
use reifydb_core::Type;
use reifydb_core::interface::{
    ActiveWriteTransaction, Table, TableId, UnversionedTransaction, VersionedTransaction,
};

pub fn create_schema<VT, UT>(atx: &mut ActiveWriteTransaction<VT, UT>, schema: &str) -> Schema
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    Catalog::create_schema(atx, SchemaToCreate { schema_span: None, name: schema.to_string() })
        .unwrap()
}

pub fn ensure_test_schema<VT, UT>(atx: &mut ActiveWriteTransaction<VT, UT>) -> Schema
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    if let Some(result) = Catalog::get_schema_by_name(atx, "test_schema").unwrap() {
        return result;
    }
    create_schema(atx, "test_schema")
}

pub fn ensure_test_table<VT, UT>(atx: &mut ActiveWriteTransaction<VT, UT>) -> Table
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    ensure_test_schema(atx);
    if let Some(result) = Catalog::get_table_by_name(atx, SchemaId(1), "test_table").unwrap() {
        return result;
    }
    create_table(atx, "test_schema", "test_table", &[])
}

pub fn create_table<VT, UT>(
    atx: &mut ActiveWriteTransaction<VT, UT>,
    schema: &str,
    table: &str,
    columns: &[table::ColumnToCreate],
) -> Table
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    Catalog::create_table(
        atx,
        TableToCreate {
            span: None,
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        },
    )
    .unwrap()
}

pub fn create_test_table_column<VT, UT>(
    atx: &mut ActiveWriteTransaction<VT, UT>,
    name: &str,
    value: Type,
    policies: Vec<ColumnPolicyKind>,
) where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    ensure_test_table(atx);

    let columns = Catalog::list_columns(atx, TableId(1)).unwrap();

    Catalog::create_column(
        atx,
        TableId(1),
        ColumnToCreate {
            span: None,
            schema_name: "test_schema",
            table: TableId(1),
            table_name: "test_table",
            column: name.to_string(),
            value,
            if_not_exists: false,
            policies,
            index: ColumnIndex(columns.len() as u16),
            auto_increment: false,
        },
    )
    .unwrap();
}
