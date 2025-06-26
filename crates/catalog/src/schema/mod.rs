// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use create::SchemaToCreate;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
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

impl Serialize for SchemaId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D>(deserializer: D) -> Result<SchemaId, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64Visitor;

        impl Visitor<'_> for U64Visitor {
            type Value = SchemaId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an unsigned 64-bit number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(SchemaId(value))
            }
        }

        deserializer.deserialize_u64(U64Visitor)
    }
}
