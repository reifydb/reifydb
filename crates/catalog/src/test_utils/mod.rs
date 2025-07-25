// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::{ColumnIndex, ColumnToCreate};
use crate::column_policy::ColumnPolicyKind;
use crate::schema::SchemaToCreate;
use crate::schema::{Schema, SchemaId};
use crate::table;
use crate::table::TableId;
use crate::table::{Table, TableToCreate};
use reifydb_core::Type;
use reifydb_core::interface::Tx;
use reifydb_storage::memory::Memory;

pub fn create_schema(tx: &mut impl Tx<Memory, Memory>, schema: &str) -> Schema {
    Catalog::create_schema(tx, SchemaToCreate { schema_span: None, name: schema.to_string() })
        .unwrap()
}

pub fn ensure_test_schema(tx: &mut impl Tx<Memory, Memory>) -> Schema {
    if let Some(result) = Catalog::get_schema_by_name(tx, "test_schema").unwrap() {
        return result;
    }
    create_schema(tx, "test_schema")
}

pub fn ensure_test_table(tx: &mut impl Tx<Memory, Memory>) -> Table {
    ensure_test_schema(tx);
    if let Some(result) = Catalog::get_table_by_name(tx, SchemaId(1), "test_table").unwrap() {
        return result;
    }
    create_table(tx, "test_schema", "test_table", &[])
}

pub fn create_table(
    tx: &mut impl Tx<Memory, Memory>,
    schema: &str,
    table: &str,
    columns: &[table::ColumnToCreate],
) -> Table {
    Catalog::create_table(
        tx,
        TableToCreate {
            span: None,
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        },
    )
    .unwrap()
}

pub fn create_test_table_column(
	tx: &mut impl Tx<Memory, Memory>,
	name: &str,
	value: Type,
	policies: Vec<ColumnPolicyKind>,
) {
    ensure_test_table(tx);

    let columns = Catalog::list_columns(tx, TableId(1)).unwrap();

    Catalog::create_column(
        tx,
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
        },
    )
    .unwrap();
}
