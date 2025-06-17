// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use create::SchemaToCreate;
use std::ops::Deref;

mod create;
mod get;
mod layout;

#[derive(Debug, PartialEq)]
pub struct Schema {
    pub id: SchemaId,
    pub name: String,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SchemaId(pub u64);

impl Deref for SchemaId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for SchemaId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl From<SchemaId> for u64 {
    fn from(value: SchemaId) -> Self {
        value.0
    }
}
