// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::column::Column;
use crate::schema::SchemaId;
pub use create::{ColumnToCreate, TableToCreate};
use std::ops::Deref;

mod create;
mod get;
mod layout;

#[derive(Debug, PartialEq)]
pub struct Table {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
    pub columns: Vec<Column>,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TableId(pub u64);

impl Deref for TableId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for TableId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl From<TableId> for u64 {
    fn from(value: TableId) -> Self {
        value.0
    }
}
