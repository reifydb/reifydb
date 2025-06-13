// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct SchemaToCreate {
    pub id: SchemaId,
    pub name: String,
}

#[repr(transparent)]
#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct TableToCreate {
    pub id: TableId,
    pub schema: SchemaId,
    pub name: String,
}
