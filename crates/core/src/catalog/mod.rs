// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct RowId(pub u64);

impl Deref for RowId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for RowId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(pub u32);

impl Deref for SchemaId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u32> for SchemaId {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

impl From<SchemaId> for u32 {
    fn from(value: SchemaId) -> Self {
        value.0
    }
}

#[derive(Debug)]
pub struct SchemaToCreate {
    pub id: SchemaId,
    pub name: String,
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct SequenceId(pub u32);

impl Deref for SequenceId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u32> for SequenceId {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u32);

impl Deref for TableId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u32> for TableId {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

impl From<TableId> for u32 {
    fn from(value: TableId) -> Self {
        value.0
    }
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
}

#[derive(Debug)]
pub struct TableToCreate {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    pub id: SchemaId,
    pub name: String,
}
