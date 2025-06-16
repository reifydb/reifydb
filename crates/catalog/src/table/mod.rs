// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use create::{TableToCreate, ColumnToCreate};
use reifydb_core::catalog::{SchemaId, TableId};

mod create;
mod layout;
mod get;

#[derive(Debug, PartialEq)]
pub struct Table {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
}
