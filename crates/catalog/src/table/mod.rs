// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::Column;
use crate::schema::SchemaId;

mod create;
mod get;
mod layout;

pub use create::{ColumnToCreate, TableToCreate};
pub use reifydb_core::interface::TableId;

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
    pub columns: Vec<Column>,
}
