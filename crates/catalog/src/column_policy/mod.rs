// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod layout;
mod list;

use crate::column::ColumnId;
use reifydb_core::diagnostic::Diagnostic;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnPolicyId(pub u64);

impl Deref for ColumnPolicyId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for ColumnPolicyId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl From<ColumnPolicyId> for u64 {
    fn from(value: ColumnPolicyId) -> Self {
        value.0
    }
}

impl Serialize for ColumnPolicyId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ColumnPolicyId {
    fn deserialize<D>(deserializer: D) -> Result<ColumnPolicyId, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64Visitor;

        impl Visitor<'_> for U64Visitor {
            type Value = ColumnPolicyId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an unsigned 64-bit number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(ColumnPolicyId(value))
            }
        }

        deserializer.deserialize_u64(U64Visitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnPolicy {
    pub id: ColumnPolicyId,
    pub column: ColumnId,
    pub policy: ColumnPolicyKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPolicyKind {
    Saturation(ColumnSaturationPolicy),
}

impl ColumnPolicyKind {
    pub fn to_u8(&self) -> (u8, u8) {
        match self {
            ColumnPolicyKind::Saturation(policy) => match policy {
                ColumnSaturationPolicy::Error => (0x01, 0x01),
                ColumnSaturationPolicy::Undefined => (0x01, 0x02),
            },
        }
    }

    pub fn from_u8(policy: u8, value: u8) -> ColumnPolicyKind {
        match (policy, value) {
            (0x01, 0x01) => ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Error),
            (0x01, 0x02) => ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined),
            _ => unimplemented!(),
        }
    }
}

impl ColumnPolicyKind {
    pub fn default_saturation_policy() -> Self {
        Self::Saturation(ColumnSaturationPolicy::default())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnSaturationPolicy {
    Error,
    // Saturate,
    // Wrap,
    // Zero,
    Undefined,
}

impl Display for ColumnPolicyKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnPolicyKind::Saturation(_) => f.write_str("saturation"),
        }
    }
}

pub const DEFAULT_COLUMN_SATURATION_POLICY: ColumnSaturationPolicy = ColumnSaturationPolicy::Error;

impl Default for ColumnSaturationPolicy {
    fn default() -> Self {
        Self::Error
    }
}

// Helper function to create column policy errors
pub fn saturation_error(diagnostic: Diagnostic) -> reifydb_core::Error {
    reifydb_core::Error(diagnostic)
}
