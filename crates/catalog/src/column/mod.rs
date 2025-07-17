// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column_policy::{ColumnPolicy, ColumnPolicyKind};
pub use create::ColumnToCreate;
use reifydb_core::Type;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
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
pub struct ColumnId(pub u64);

impl Deref for ColumnId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for ColumnId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl From<ColumnId> for u64 {
    fn from(value: ColumnId) -> Self {
        value.0
    }
}

impl Serialize for ColumnId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ColumnId {
    fn deserialize<D>(deserializer: D) -> Result<ColumnId, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64Visitor;

        impl Visitor<'_> for U64Visitor {
            type Value = ColumnId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an unsigned 64-bit number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(ColumnId(value))
            }
        }

        deserializer.deserialize_u64(U64Visitor)
    }
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
