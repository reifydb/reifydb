// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column_policy::{ColumnPolicy, ColumnPolicyKind};
pub use create::ColumnToCreate;
pub use reifydb_core::interface::ColumnId;
use reifydb_core::Type;
use std::ops::Deref;

mod create;
mod get;
mod layout;
mod list;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub ty: Type,
    pub policies: Vec<ColumnPolicy>,
    pub index: ColumnIndex,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnIndex(pub u16);

impl Deref for ColumnIndex {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u16> for ColumnIndex {
    fn eq(&self, other: &u16) -> bool {
        self.0.eq(other)
    }
}

impl From<ColumnIndex> for u16 {
    fn from(value: ColumnIndex) -> Self {
        value.0
    }
}
