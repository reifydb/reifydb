// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{ColumnIndex, ColumnToCreate};
use crate::schema::SchemaToCreate;
use crate::schema::{Schema, SchemaId};
use crate::table::TableToCreate;
use crate::{Catalog, table};
use reifydb_core::Type;
use reifydb_core::interface::{
    ActiveCommandTransaction, ColumnPolicyKind, Table, TableId, Transaction,
};

pub fn create_schema<T>(txn: &mut ActiveCommandTransaction<T>, schema: &str) -> Schema
where
    T: Transaction,
{
    Catalog::create_schema(txn, SchemaToCreate { schema_span: None, name: schema.to_string() })
        .unwrap()
}

pub fn ensure_test_schema<T>(txn: &mut ActiveCommandTransaction<T>) -> Schema
where
    T: Transaction,
{
    if let Some(result) = Catalog::get_schema_by_name(txn, "test_schema").unwrap() {
        return result;
    }
    create_schema(txn, "test_schema")
}

pub fn ensure_test_table<T>(txn: &mut ActiveCommandTransaction<T>) -> Table
where
    T: Transaction,
    
{
    ensure_test_schema(txn);
    if let Some(result) = Catalog::get_table_by_name(txn, SchemaId(1), "test_table").unwrap() {
        return result;
    }
    create_table(txn, "test_schema", "test_table", &[])
}

pub fn create_table<T>(
    txn: &mut ActiveCommandTransaction<T>,
    schema: &str,
    table: &str,
    columns: &[table::ColumnToCreate],
) -> Table
where
    T: Transaction,
    
{
    Catalog::create_table(
        txn,
        TableToCreate {
            span: None,
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        },
    )
    .unwrap()
}

pub fn create_test_table_column<T>(
    txn: &mut ActiveCommandTransaction<T>,
    name: &str,
    value: Type,
    policies: Vec<ColumnPolicyKind>,
) where
    T: Transaction,
    
{
    ensure_test_table(txn);

    let columns = Catalog::list_columns(txn, TableId(1)).unwrap();

    Catalog::create_column(
        txn,
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
