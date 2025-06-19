// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::delta::Delta;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Version};
use reifydb_storage::Versioned;
use std::cmp;
use std::cmp::Reverse;

pub enum TransactionValue {
    PendingIter { version: Version, key: EncodedKey, row: EncodedRow },
    Pending(Pending),
    Committed(Committed),
}

impl From<Versioned> for TransactionValue {
    fn from(value: Versioned) -> Self {
        Self::Committed(Committed { key: value.key, row: value.row, version: value.version })
    }
}

impl core::fmt::Debug for TransactionValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TransactionValue")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", &self.row())
            .finish()
    }
}

impl Clone for TransactionValue {
    fn clone(&self) -> Self {
        match self {
            Self::Committed(item) => Self::Committed(item.clone()),
            Self::Pending(delta) => Self::Pending(delta.clone()),
            Self::PendingIter { version, key, row: value } => {
                Self::PendingIter { version: *version, key: key.clone(), row: value.clone() }
            }
        }
    }
}

impl TransactionValue {
    pub fn key(&self) -> &EncodedKey {
        match self {
            Self::PendingIter { key, .. } => key,
            Self::Pending(item) => item.key(),
            Self::Committed(item) => item.key(),
        }
    }

    pub fn version(&self) -> Version {
        match self {
            Self::PendingIter { version, .. } => *version,
            Self::Pending(item) => item.version(),
            Self::Committed(item) => item.version(),
        }
    }

    pub fn row(&self) -> &EncodedRow {
        match self {
            Self::PendingIter { row, .. } => row,
            Self::Pending(item) => item.row().expect("row of pending cannot be `None`"),
            Self::Committed(item) => &item.row,
        }
    }

    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

impl From<(Version, EncodedKey, EncodedRow)> for TransactionValue {
    fn from((version, k, b): (Version, EncodedKey, EncodedRow)) -> Self {
        Self::PendingIter { version, key: k, row: b }
    }
}

impl From<(Version, &EncodedKey, &EncodedRow)> for TransactionValue {
    fn from((version, k, b): (Version, &EncodedKey, &EncodedRow)) -> Self {
        Self::PendingIter { version, key: k.clone(), row: b.clone() }
    }
}

impl From<Pending> for TransactionValue {
    fn from(pending: Pending) -> Self {
        Self::Pending(pending)
    }
}

impl From<Committed> for TransactionValue {
    fn from(item: Committed) -> Self {
        Self::Committed(item)
    }
}

#[derive(Clone, Debug)]
pub struct Committed {
    pub(crate) key: EncodedKey,
    pub(crate) row: EncodedRow,
    pub(crate) version: Version,
}

impl From<Versioned> for Committed {
    fn from(value: Versioned) -> Self {
        Self { key: value.key, row: value.row, version: value.version }
    }
}

impl Committed {
    pub fn key(&self) -> &EncodedKey {
        &self.key
    }

    pub fn row(&self) -> &EncodedRow {
        &self.row
    }

    pub fn version(&self) -> Version {
        self.version
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pending {
    pub delta: Delta,
    pub version: Version,
}

impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.delta
            .key()
            .cmp(other.delta.key())
            .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
}

impl Clone for Pending {
    fn clone(&self) -> Self {
        Self { version: self.version, delta: self.delta.clone() }
    }
}

impl Pending {
    pub fn delta(&self) -> &Delta {
        &self.delta
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn into_components(self) -> (u64, Delta) {
        (self.version, self.delta)
    }

    pub fn key(&self) -> &EncodedKey {
        self.delta.key()
    }

    pub fn row(&self) -> Option<&EncodedRow> {
        self.delta.row()
    }

    pub fn was_removed(&self) -> bool {
        matches!(self.delta, Delta::Remove { .. })
    }
}
