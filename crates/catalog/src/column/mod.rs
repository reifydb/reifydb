// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use create::ColumnToCreate;
pub use policy::*;
use reifydb_core::ValueKind;
use std::ops::Deref;

mod create;
mod get;
mod layout;
mod policy;

#[derive(Debug, PartialEq)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub value: ValueKind,
    pub policies: Vec<ColumnPolicy>,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnId(pub u32);

impl Deref for ColumnId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u32> for ColumnId {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

impl From<ColumnId> for u32 {
    fn from(value: ColumnId) -> Self {
        value.0
    }
}
