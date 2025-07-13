// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt;
use std::ops::Deref;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
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

impl Serialize for RowId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for RowId {
    fn deserialize<D>(deserializer: D) -> Result<RowId, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64Visitor;

        impl Visitor<'_> for U64Visitor {
            type Value = RowId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an unsigned 64-bit number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(RowId(value))
            }
        }
        deserializer.deserialize_u64(U64Visitor)
    }
}
