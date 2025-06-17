// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod create;
mod layout;
mod list;

use crate::column::ColumnId;
use reifydb_diagnostic::Diagnostic;
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

#[derive(Debug, PartialEq)]
pub enum ColumnPolicyError {
    Saturation(Diagnostic),
}

impl ColumnPolicyError {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            ColumnPolicyError::Saturation(diagnostic) => diagnostic,
        }
    }
}
